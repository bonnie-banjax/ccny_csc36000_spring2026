from __future__ import annotations

import threading
from typing import Any, Protocol


SHARD_LOCK = threading.Lock()


class CoordinatorTransport(Protocol):
    def apply_to_shard(
        self,
        logical_shard_id: int,
        operation_name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        ...

    def read_from_shard(
        self,
        logical_shard_id: int,
        query_name: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        ...


class RouterView(Protocol):
    def logical_shard_for_payload(
        self,
        application_name: str,
        operation_name: str,
        payload: dict[str, Any],
    ) -> int:
        ...

    def owner_addr_for_logical_shard(self, logical_shard_id: int) -> str:
        ...


def execute_gateway_request(
    application_name: str,
    operation_name: str,
    payload: dict[str, Any],
    router: RouterView,
    transport: CoordinatorTransport,
) -> dict[str, Any]:
    """
    Gateway-side request execution.

    Important behavior:
    - route inventory requests by item_id through the router
    - let invalid operations raise exceptions instead of hiding them
    """
    if application_name != "inventory":
        raise NotImplementedError(
            f"Gateway currently only supports inventory, got {application_name!r}"
        )

    logical_shard_id = router.logical_shard_for_payload(
        application_name=application_name,
        operation_name=operation_name,
        payload=payload,
    )

    mutation_ops = {"create_item", "reserve_item", "release_reservation"}
    query_ops = {"get_inventory"}

    if operation_name in mutation_ops:
        result = transport.apply_to_shard(
            logical_shard_id=logical_shard_id,
            operation_name=operation_name,
            payload=payload,
        )
    elif operation_name in query_ops:
        result = transport.read_from_shard(
            logical_shard_id=logical_shard_id,
            query_name=operation_name,
            payload=payload,
        )
    else:
        raise ValueError(f"Unknown operation: {operation_name}")

    if not isinstance(result, dict):
        raise TypeError("Invalid response from shard")

    shard_addr = router.owner_addr_for_logical_shard(logical_shard_id)
    routing_info = {
        "logical_shard_id": logical_shard_id,
        "storage_addr": shard_addr,
    }

    # Match gateway response expectations
    if operation_name == "create_item":
        result["routing"] = routing_info
    else:
        result["served_by"] = [routing_info]

    return result


def apply_local_mutation(
    state: dict[str, Any],
    operation_name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Shard-local mutation execution for inventory.

    Isolation:
      - one shard-local lock
      - mutations execute one at a time

    Atomicity:
      - mutate loaded in-memory state
      - shard server only persists after successful return
      - if we raise, bad state is not saved
    """
    with SHARD_LOCK:
        inventory = state.setdefault("inventory", {})

        if operation_name == "create_item":
            item_id = payload["item_id"]
            quantity = int(payload["quantity"])

            if item_id not in inventory:
                inventory[item_id] = {
                    "total_quantity": quantity,
                    "reservations": {},
                }

            return {
                "item_id": item_id,
                "quantity": inventory[item_id]["total_quantity"],
            }

        if operation_name == "reserve_item":
            item_id = payload["item_id"]
            reservation_id = payload["reservation_id"]
            requested_quantity = int(payload["quantity"])

            if item_id not in inventory:
                raise ValueError(f"Item {item_id} not found")

            item = inventory[item_id]

            # Optional retry-safe behavior: duplicate reservation id is treated
            # as already committed rather than double-applying.
            if reservation_id in item["reservations"]:
                reserved_quantity = sum(item["reservations"].values())
                remaining_quantity = item["total_quantity"] - reserved_quantity
                return {
                    "committed": True,
                    "remaining_quantity": remaining_quantity,
                }

            reserved_quantity = sum(item["reservations"].values())
            available_quantity = item["total_quantity"] - reserved_quantity

            if requested_quantity > available_quantity:
                raise ValueError("Insufficient inventory available")

            item["reservations"][reservation_id] = requested_quantity

            return {
                "committed": True,
                "remaining_quantity": available_quantity - requested_quantity,
            }

        if operation_name == "release_reservation":
            item_id = payload["item_id"]
            reservation_id = payload["reservation_id"]

            if item_id not in inventory:
                raise ValueError(f"Item {item_id} not found")

            item = inventory[item_id]

            if reservation_id not in item["reservations"]:
                raise ValueError(
                    f"Reservation {reservation_id} not found for item {item_id}"
                )

            del item["reservations"][reservation_id]

            reserved_quantity = sum(item["reservations"].values())
            remaining_quantity = item["total_quantity"] - reserved_quantity

            return {
                "committed": True,
                "remaining_quantity": remaining_quantity,
            }

        raise NotImplementedError(f"Unknown mutation: {operation_name}")


def run_local_query(
    state: dict[str, Any],
    query_name: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Shard-local read execution for inventory.
    We use the same lock so reads do not observe half-finished updates.
    """
    with SHARD_LOCK:
        inventory = state.get("inventory", {})

        if query_name == "get_inventory":
            item_id = payload["item_id"]

            if item_id not in inventory:
                raise ValueError(f"Item {item_id} not found")

            item = inventory[item_id]
            total_quantity = item["total_quantity"]
            reserved_quantity = sum(item["reservations"].values())
            available_quantity = total_quantity - reserved_quantity

            return {
                "item_id": item_id,
                "total_quantity": total_quantity,
                "reserved_quantity": reserved_quantity,
                "available_quantity": available_quantity,
            }

        raise NotImplementedError(f"Unknown query: {query_name}")