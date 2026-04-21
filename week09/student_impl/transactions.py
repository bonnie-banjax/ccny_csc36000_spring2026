from __future__ import annotations

from typing import Any, Protocol

import threading # ORD
import json
from typing import Any
from student_impl.storage import save_logical_shard_state

# A global lock or a per-shard lock registry is needed.
# For the MVP, a simple lock ensures serializable isolation within the process.
SHARD_LOCK = threading.Lock()

class CoordinatorTransport(Protocol):
    def apply_to_shard(self, logical_shard_id: int, operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
      ...

    def read_from_shard(self, logical_shard_id: int, query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
      ...


class RouterView(Protocol):
    def logical_shard_for_payload(self, application_name: str, operation_name: str, payload: dict[str, Any]) -> int:
      ...

    def owner_addr_for_logical_shard(self, logical_shard_id: int) -> str:
      ...


def execute_gateway_request(
  application_name: str,
  operation_name: str,
  payload: dict[str, Any],
  router: Any, # RouterView Protocol
  transport: Any, # CoordinatorTransport Protocol
) -> dict[str, Any]:
  """
  Orchestrates the transaction from the Gateway's perspective.
  """
  if application_name != "inventory":
    raise NotImplementedError("Gateway currently only configured for inventory application.")

  # 1. Routing: Determine which shard to talk to
  # We extract the item_id from the payload to find the partition key
  try:
    part_key = build_partition_key(application_name, operation_name, payload)
    logical_shard_id = choose_logical_shard(part_key)
  except (ValueError, KeyError) as e:
    # If we can't find an item_id, we can't route the request
    return {"ok": False, "error": f"Routing failed: {str(e)}"}

  # 2. Dispatch: Distinguish between Reads and Mutations
  # Based on the Week09Gateway service definition in the .proto
  mutation_ops = {"CreateInventoryItem", "ReserveItem", "ReleaseReservation"}
  query_ops = {"GetInventory"}

  try:
    if operation_name in mutation_ops:
      # Mutations go through apply_to_shard (ShardNode.Apply)
      result = transport.apply_to_shard(
        logical_shard_id=logical_shard_id,
        operation_name=operation_name,
        payload=payload
      )
    elif operation_name in query_ops:
      # Queries go through read_from_shard (ShardNode.Read)
      result = transport.read_from_shard(
        logical_shard_id=logical_shard_id,
        query_name=operation_name,
        payload=payload
      )
    else:
      return {"ok": False, "error": f"Unknown operation: {operation_name}"}

    # 3. Post-Processing: Inject routing metadata for the client response
    # The .proto messages (e.g., ReserveItemResponse) expect a 'served_by' list
    shard_addr = router.owner_addr_for_logical_shard(logical_shard_id)
    routing_info = {
      "logical_shard_id": logical_shard_id,
      "storage_addr": shard_addr
    }

    # Ensure the response format matches what the Gateway RPC expects
    # Note: transport calls usually return the 'result_json' parsed into a dict
    if isinstance(result, dict):
      # If the application response expects a list of RoutingInfo
      if "served_by" not in result and operation_name != "CreateInventoryItem":
         result["served_by"] = [routing_info]
      # Create responses usually take a single RoutingInfo object
      elif operation_name == "CreateInventoryItem":
         result["routing"] = routing_info

      return result

    return {"ok": False, "error": "Invalid response from shard"}

  except Exception as e:
    # This catches network timeouts or Shard crashes
    return {"ok": False, "error": f"Shard communication error: {str(e)}"}


def apply_local_mutation(state: dict[str, Any], operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
  """
  Executes inventory mutations with strict serializable isolation.
  Expected to be called by the ShardNode implementation.
  """
  # Ensure only one mutation happens at a time on this shard
  with SHARD_LOCK:
    inventory = state.setdefault("inventory", {})

    if operation_name == "CreateInventoryItem":
      item_id = payload["item_id"]
      quantity = payload["quantity"]
      # Idempotency: if it exists, we just return the current state
      if item_id not in inventory:
        inventory[item_id] = {
          "total_quantity": quantity,
          "reservations": {}
        }
      return {"item_id": item_id, "quantity": inventory[item_id]["total_quantity"]}

    elif operation_name == "ReserveItem":
      item_id = payload["item_id"]
      res_id = payload["reservation_id"]
      req_qty = payload["quantity"]

      if item_id not in inventory:
        raise ValueError(f"Item {item_id} not found")

      item = inventory[item_id]

      # Idempotency check: is this reservation already recorded?
      if res_id in item["reservations"]:
        # If it already exists, we treat it as a success (idempotent)
        current_reserved = sum(item["reservations"].values())
        return {"committed": True, "remaining_quantity": item["total_quantity"] - current_reserved}

      # Safety Check: Invariant enforcement
      current_reserved = sum(item["reservations"].values())
      available = item["total_quantity"] - current_reserved

      if req_qty > available:
        # This triggers an exception that the ShardNode should catch and return in ApplyResponse.error
        raise ValueError("Insufficient inventory available")

      # Mutation
      item["reservations"][res_id] = req_qty

      # THE FLUSH: We do not return until the change is durable.
      # TODO In a real impl, the storage_path would be passed or known by the shard context.
      # Assuming the Shard Server handles the save() call immediately after this returns.
      return {"committed": True, "remaining_quantity": available - req_qty}

    elif operation_name == "ReleaseReservation":
      item_id = payload["item_id"]
      res_id = payload["reservation_id"]

      if item_id not in inventory or res_id not in inventory[item_id]["reservations"]:
        raise ValueError(f"Reservation {res_id} not found for item {item_id}")

      item = inventory[item_id]
      del item["reservations"][res_id]

      new_available = item["total_quantity"] - sum(item["reservations"].values())
      return {"committed": True, "remaining_quantity": new_available}

    else:
      raise NotImplementedError(f"Unknown mutation: {operation_name}")


def run_local_query(state: dict[str, Any], query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
  """Reads state. Since it's a dict, we still use the lock to prevent reading mid-mutation."""
  with SHARD_LOCK:
    inventory = state.get("inventory", {})
    if query_name == "GetInventory":
      item_id = payload["item_id"]
      if item_id not in inventory:
        raise ValueError(f"Item {item_id} not found")

      item = inventory[item_id]
      total = item["total_quantity"]
      reserved = sum(item["reservations"].values())
      return {
        "item_id": item_id,
        "total_quantity": total,
        "reserved_quantity": reserved,
        "available_quantity": total - reserved
      }
    raise NotImplementedError(f"Unknown query: {query_name}")

