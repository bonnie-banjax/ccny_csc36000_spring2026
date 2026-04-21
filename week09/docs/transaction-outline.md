---
title: "Transaction Docs"
---


# TRANSACTION DOCS
<!--|======[1]======[2]======[3]======[>X<]======[5]======[6]======[7]======|-->

## 1. Schema Design / Data Model Example

### **Overview**
The system utilizes a **document-oriented state model** structured as a nested JSON dictionary. This model is designed specifically for **Application C (Inventory)**, where the primary objective is to manage a finite resource (`total_quantity`) against multiple concurrent claims (`reservations`).

```json
{
  "inventory": {
    "item-123": {
      "total_quantity": 10,
      "reservations": {
        "res-abc": 2,
        "res-def": 1
      }
    }
  }
}
```



### **Code Logic Analysis**
The state is partitioned at the top level by a functional domain key (`"inventory"`). Each entry within this domain is keyed by a unique `item_id`.
* **`total_quantity`**: An integer representing the authoritative supply.
* **`reservations`**: A sub-dictionary mapping a `reservation_id` to a specific `quantity`. 

This structure allows the system to calculate the "available" balance dynamically:
$$\text{Available} = \text{total\_quantity} - \sum(\text{reservations.values()})$$

### **Missing Dependencies & Context**
While the snippet provides a clear example, the following elements are necessary for full integration:
1.  **Schema Validation**: There is no logic presented to enforce that `total_quantity` remains a non-negative integer or to prevent the injection of malformed keys outside of the `"inventory"` block.
2.  **Versioning/Sequence Numbers**: The model lacks a `version` or `last_modified_index` field. Without this, the system cannot easily implement Optimistic Concurrency Control (OCC) or verify if the in-memory state is ahead of the disk state during a race condition.

### **Implementation Considerations**
* **Atomicity of Nested Updates**: Because the entire item object (including all its reservations) is stored under a single key, any update to a reservation requires a rewrite of the entire item entry. This is efficient for small numbers of reservations but may lead to performance degradation if an item has thousands of active reservations (a "hot key" scenario).
* **Idempotency Support**: By using a dictionary for `reservations` where the key is a unique ID provided by the client, the model inherently supports idempotency. If a client retries a `ReserveItem` request with the same `res-abc`, the system can detect the existing entry rather than double-counting the reservation.
* **Memory Efficiency**: The model assumes the entire shard state fits in memory. For a large-scale inventory, a more granular approach (splitting reservations into separate keys) might be required to avoid high memory overhead during serialization.

<!--|======[1]======[2]======[3]======[>X<]======[5]======[6]======[7]======|-->

## 2. Shard State Persistence & Lifecycle Management

### **Overview**
The `ShardPersistenceManager` acts as the intermediary between the system's in-memory "source of truth" and the provided `student_impl.storage` API. Its primary role is to abstract the serialization of the inventory schema and ensure that each logical shard starts from a consistent, valid state, whether it is a fresh initialization or a recovery from a crash.


```python

# student_impl/storage_helpers.py (Internal helper for Stage 1)
from __future__ import annotations
from pathlib import Path
from typing import Any
from student_impl.storage import load_logical_shard_state, save_logical_shard_state

def get_empty_shard_state() -> dict[str, Any]:
  """Returns a fresh schema for a new logical shard."""
  return {
    "inventory": {} # item_id -> {total_quantity, reservations: {res_id -> qty}}
  }

class ShardPersistenceManager:
  """Manages the lifecycle of a single logical shard's durable data."""

  def __init__(self, storage_path: Path):
    self.storage_path = storage_path
    self._state = self.load()

  def load(self) -> dict[str, Any]:
    """Loads state from disk or initializes if empty."""
    state = load_logical_shard_state(self.storage_path)
    if not state:
      return get_empty_shard_state()
    # Ensure the expected top-level key exists
    if "inventory" not in state:
      state["inventory"] = {}
    return state

  def save(self, state: dict[str, Any]) -> None:
    """Flushes the entire state to disk safely."""
    save_logical_shard_state(self.storage_path, state)
    self._state = state

  @property
  def state(self) -> dict[str, Any]:
    return self._state
```



### **Code Logic Analysis**
* **Initialization & Recovery**: The `load()` method implements a "lazy initialization" pattern. It attempts to fetch state from disk using the required `load_logical_shard_state` function. If the disk is empty (returning `None`), it invokes `get_empty_shard_state()` to generate a schema-compliant dictionary.
* **Schema Enforcement**: It performs a basic structural check during loading (`if "inventory" not in state`) to ensure that even if the disk contains a valid JSON object, it conforms to the specific expected application namespace.
* **State Encapsulation**: By maintaining an internal `_state` property, the manager provides a consistent handle for the Shard Node to interact with, acting as the bridge between the logic layer and the physical I/O layer.

### **Missing Dependencies & Context**
1.  **Atomic Write-Behind**: The `save()` method calls `save_logical_shard_state`, but it does not implement any "temp-file and rename" logic or checksumming. It relies entirely on the provided storage layer to ensure that the file is not corrupted if the process crashes mid-write.
2.  **Concurrency at the Persistence Layer**: While the manager stores the state, it does not manage file-level locks. If two different managers were pointed at the same `storage_path`, they would overwrite each other's data (though the `ShardNodeManager` in later snippets aims to prevent this by assigning unique paths).
3.  **The `Path` Provider**: The snippet assumes a `storage_path` is passed in, but the logic for determining *where* these files live (the directory hierarchy) is handled by the broader system configuration.

### **Implementation Considerations**
* **Write Amplification**: This manager flushes the *entire* shard state to disk for every mutation. In a production environment with many items, this is inefficient. However, for the project's "MVP" requirements, it is a robust way to guarantee that the `inventory` and `reservations` are committed together as a single atomic unit.
* **Recovery Reliability**: This component is the backbone of the "Failure Handling" requirement. On restart, the `load()` method ensures that the node can resume exactly where it left off, effectively "shrinking" the window of potential data loss to the time between a successful mutation and the completion of the `save()` call.

---

<!--|======[1]======[2]======[3]======[>X<]======[5]======[6]======[7]======|-->


## 3. Intra-Shard Concurrency Control & Local Mutation Logic

### **Overview**
This component provides the core **Concurrency Control** and **Transactional Safety** for the shard. It implements the "Per-shard serial execution" strategy mentioned in the Project Brief. By wrapping all data access in a mutex, it ensures **Serializable Isolation**—the highest level of isolation—preventing anomalies like lost updates or inconsistent reads within the context of a single logical shard.


```python

import threading
import json
from typing import Any
from student_impl.storage import save_logical_shard_state

# A global lock or a per-shard lock registry is needed. 
# For the MVP, a simple lock ensures serializable isolation within the process.
SHARD_LOCK = threading.Lock()

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

```



### **Code Logic Analysis**
* **The Mutex (`SHARD_LOCK`)**: A global `threading.Lock()` (which should be scoped per-shard in a multi-shard environment) acts as a monitor. No two operations can execute `apply_local_mutation` or `run_local_query` simultaneously, effectively serializing the request stream.
* **Guard-Based Mutation**: The `ReserveItem` operation demonstrates a rigorous check-before-act pattern:
    1.  **Existence Check**: Validates the `item_id`.
    2.  **Idempotency Check**: Uses the `reservation_id` to ensure a re-sent request doesn't double-reserve—crucial for handling the "Client retries" mentioned in the Failure Model.
    3.  **Invariant Guard**: Sums existing reservations to ensure the new request does not exceed `total_quantity`.
* **Atomic State Transition**: Because the mutation happens in-memory within the lock's scope, the transition from "Available" to "Reserved" is atomic to any concurrent observer.

### **Missing Dependencies & Context**
1.  **Scope of `SHARD_LOCK`**: In the snippet, `SHARD_LOCK` is a single global instance. In a real implementation of `ShardNodeManager`, there should be a unique lock per `logical_shard_id` to allow parallel processing across different shards hosted on the same physical node.
2.  **Explicit Durability Trigger**: The snippet notes a `TODO` for the flush. While the logic modifies the `state` dict, the actual write to disk (the "Commit") is not triggered inside this function, creating a dependency on the caller (the `ShardNode`) to execute the `save()` call immediately before releasing the lock.
3.  **Exception Mapping**: The use of `ValueError` is internal. These must be caught and translated into the specific error codes required by the project's `.proto` or RPC contracts.

### **Implementation Considerations**
* **Performance Bottleneck**: Serial execution is simple and correct but limits throughput to the speed of a single thread and the underlying disk I/O. For high-contention items, this shard will become a "Hot Shard."
* **Read Isolation**: By including `run_local_query` under the lock, the system prevents **Dirty Reads**. A client cannot query the inventory while a reservation is halfway through being processed.

---

<!--|======[1]======[2]======[3]======[>X<]======[5]======[6]======[7]======|-->

## 4. Distributed Request Orchestrator & Shard Router

### **Overview**
The `execute_gateway_request` function serves as the **Transaction Coordinator** and **Routing Engine**. It acts as the "brain" of the gateway, abstracting the complexity of the distributed shard layout from the application client. Its primary responsibility is to resolve a high-level application request into a specific network call to the correct shard.


```python
import json
from typing import Any
from student_impl.sharding import build_partition_key, choose_logical_shard

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
```



### **Code Logic Analysis**
* **Partition Key Resolution**: It utilizes `build_partition_key` to extract the `item_id` and `choose_logical_shard` to determine the destination. This implements the **Directory/Hash-based sharding** required by the brief.
* **Operational Dispatch**: It categorizes incoming requests into **Mutations** (which change state) and **Queries** (which only read). Mutations are routed to `apply_to_shard` (mapping to a gRPC `Apply` call), while queries go to `read_from_shard` (mapping to `Read`).
* **Metadata Injection**: Post-execution, it enriches the response with `served_by` metadata. This satisfies the requirement that the system makes the "shard mapping visible" and allows the client to see exactly which physical node handled their request.

### **Missing Dependencies & Context**
1.  **Transport Protocol**: The `transport` object is a dependency that must implement a communication interface (likely gRPC or a similar RPC framework). The logic assumes this transport handles the low-level details of networking and serialization.
2.  **Retry/Timeout Logic**: While the brief mentions "client retries," this gateway snippet lacks explicit logic to retry a request if a shard is temporarily unreachable. It currently catches exceptions and returns an "ok: False" error immediately.
3.  **Global Shard Map**: The `router` object is assumed to have a consistent view of which `storage_addr` (IP/Port) corresponds to each `logical_shard_id`. The snippet does not show how this map is updated or synchronized across multiple gateways.

### **Implementation Considerations**
* **Single-Shard Limitation**: This implementation is optimized for **Single-Shard Transactions**. Because it routes based on a single `item_id`, it cannot natively handle a transaction that touches two items on different shards (e.g., a "Transfer" between items) without a more complex protocol like Two-Phase Commit (2PC).
* **Error Propagation**: By catching `Exception` and returning it as a JSON error, the gateway ensures the system remains available even if a single shard node crashes, though that specific request will fail.

---


<!--|======[1]======[2]======[3]======[>X<]======[5]======[6]======[7]======|-->

## 5. Multi-Shard Node Controller & Recovery Engine

### **Overview**
The `ShardNodeManager` is the top-level organizational component residing on each physical server. It is responsible for the **physical-to-logical mapping**—managing multiple logical shards within a single process. Its most critical roles are the restoration of data integrity during the startup "Recovery" phase and the orchestration of the **Durable Commit** path during active operations.


```python

# student_impl/shard_context.py (Conceptual integration)
from pathlib import Path
from student_impl.storage import load_logical_shard_state, save_logical_shard_state
from student_impl.transactions import apply_local_mutation, run_local_query

class ShardNodeManager:
  def __init__(self, node_id: int, storage_dir: Path):
    self.node_id = node_id
    self.storage_dir = storage_dir
    # Map of logical_shard_id -> in-memory dict state
    self.shards: dict[int, dict] = {}
    self.storage_paths: dict[int, Path] = {}

  def recover_shards(self, assigned_shard_ids: list[int]):
    """Reloads all assigned logical shards from disk on startup."""
    for shard_id in assigned_shard_ids:
      path = self.storage_dir / f"shard_{shard_id}.json"
      self.storage_paths[shard_id] = path

      # Load from disk using the Stage 1/Storage interface
      state = load_logical_shard_state(path)

      # If disk is empty, initialize the schema
      if not state:
        state = {"inventory": {}}

      self.shards[shard_id] = state
    print(f"Node {self.node_id} recovered {len(self.shards)} shards.")

  def handle_apply(self, shard_id: int, op: str, payload: dict) -> dict:
    """The entry point for the ShardNode.Apply gRPC call."""
    if shard_id not in self.shards:
      raise ValueError(f"Shard {shard_id} not hosted on this node")

    # 1. Execute mutation (Stage 2 Logic)
    # apply_local_mutation handles the internal SHARD_LOCK
    result = apply_local_mutation(self.shards[shard_id], op, payload)

    # 2. Immediate Durability (The 'MVP' Flush)
    # Since apply_local_mutation modified the dict, we persist immediately.
    save_logical_shard_state(self.storage_paths[shard_id], self.shards[shard_id])

    return result
```



### **Code Logic Analysis**
* **The Recovery Flow**: The `recover_shards` method is the implementation of the "Failure Handling" requirement from the project brief. It iterates through assigned `shard_ids`, locates their specific `.json` files on disk, and reconstructs the in-memory state. This ensures that a node can crash and restart without violating the durability of previously committed transactions.
* **Orchestrated Apply**: The `handle_apply` method serves as the bridge between the gRPC entry point and the local mutation logic. It performs a vital three-step sequence:
    1.  **Context Validation**: Confirms the node is actually responsible for the requested shard ID.
    2.  **State Transition**: Delegates the logic to `apply_local_mutation`.
    3.  **Atomic Persistence**: Immediately flushes the mutated state to disk via `save_logical_shard_state` *before* returning success to the Gateway.
* **Data Isolation**: By maintaining a `shards` dictionary (logical_id -> state) and a `storage_paths` dictionary, it ensures that logical shards remain independent and do not leak data across boundaries.

### **Missing Dependencies & Context**
1.  **Shard Assignment Mechanism**: The `assigned_shard_ids` are passed in during recovery, but the snippet does not show the "Membership Provider" or configuration file that tells this node which shards it owns.
2.  **Graceful Shutdown**: There is no logic for a "Clean Exit." While the system is designed to handle crashes, a production implementation would include a way to flush all shards and release file handles gracefully.
3.  **Concurrency Mapping**: As noted in the mutation documentation, while `apply_local_mutation` uses a lock, the `ShardNodeManager` needs to ensure that the `SHARD_LOCK` is correctly shared or partitioned so that operations on Shard 1 do not unnecessarily block operations on Shard 2.

### **Implementation Considerations**
* **Durability Guarantee**: This manager enforces the "MVP Flush." By calling `save` inside `handle_apply`, it guarantees that if the Gateway receives an "OK" response, the data is physically on disk. This is the "All-or-Nothing" behavior required by the brief.
* **Disk I/O Contention**: Because all logical shards might share the same physical disk (`storage_dir`), high write volume across multiple shards can lead to I/O wait times. The manager is the logical place where one would eventually implement a more sophisticated write-ahead log (WAL) to mitigate this.

---

<!--|======[1]======[2]======[3]======[>X<]======[5]======[6]======[7]======|-->

# END


