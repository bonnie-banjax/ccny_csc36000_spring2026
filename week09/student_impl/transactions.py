from __future__ import annotations

from typing import Any, Protocol


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


# Operations that are pure reads (no state mutation).
_READ_OPERATIONS = {"get_inventory"}


def execute_gateway_request(
    application_name: str,
    operation_name: str,
    payload: dict[str, Any],
    router: RouterView,
    transport: CoordinatorTransport,
) -> dict[str, Any]:
    """
    Route one inventory request to the correct logical shard.

    Every inventory operation is keyed on item_id, so it always lands on a
    single shard — no cross-shard coordination is needed.
    """
    shard_id = router.logical_shard_for_payload(application_name, operation_name, payload)
    if operation_name in _READ_OPERATIONS:
        return transport.read_from_shard(shard_id, operation_name, payload)
    return transport.apply_to_shard(shard_id, operation_name, payload)


def apply_local_mutation(state: dict[str, Any], operation_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Apply a single-shard mutation to local shard state and return a
    JSON-serializable result.

    ``state`` is mutated in-place; the caller (shard_server) persists it
    after this function returns.

    Supported operations
    --------------------
    create_item
        payload: {item_id, quantity}
        Creates a new inventory item.  Raises ValueError if the item
        already exists.

    reserve_item
        payload: {item_id, reservation_id, quantity}
        Reserves ``quantity`` units under ``reservation_id``.
        Raises ValueError when available_quantity < requested quantity.

    release_reservation
        payload: {item_id, reservation_id}
        Removes an existing reservation.
        Raises KeyError if the reservation does not exist.
    """
    inventory: dict[str, Any] = state.setdefault("inventory", {})

    if operation_name == "create_item":
        item_id = payload["item_id"]
        quantity = int(payload["quantity"])
        if item_id in inventory:
            raise ValueError(f"Item {item_id!r} already exists")
        inventory[item_id] = {"total_quantity": quantity, "reservations": {}}
        return {"item_id": item_id, "quantity": quantity}

    if operation_name == "reserve_item":
        item_id = payload["item_id"]
        reservation_id = payload["reservation_id"]
        quantity = int(payload["quantity"])

        item = inventory.get(item_id)
        if item is None:
            raise KeyError(f"Item {item_id!r} not found")

        currently_reserved = sum(item["reservations"].values())
        available = item["total_quantity"] - currently_reserved
        if quantity > available:
            raise ValueError(
                f"Insufficient inventory for {item_id!r}: "
                f"{available} available, {quantity} requested"
            )

        item["reservations"][reservation_id] = quantity
        new_reserved = currently_reserved + quantity
        return {
            "committed": True,
            "remaining_quantity": item["total_quantity"] - new_reserved,
        }

    if operation_name == "release_reservation":
        item_id = payload["item_id"]
        reservation_id = payload["reservation_id"]

        item = inventory.get(item_id)
        if item is None:
            raise KeyError(f"Item {item_id!r} not found")
        if reservation_id not in item["reservations"]:
            raise KeyError(
                f"Reservation {reservation_id!r} not found for item {item_id!r}"
            )

        del item["reservations"][reservation_id]
        reserved = sum(item["reservations"].values())
        return {
            "committed": True,
            "remaining_quantity": item["total_quantity"] - reserved,
        }

    raise NotImplementedError(f"Unknown mutation operation: {operation_name!r}")


def run_local_query(state: dict[str, Any], query_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    """
    Execute a single-shard read against local shard state and return a
    JSON-serializable result.

    Supported queries
    -----------------
    get_inventory
        payload: {item_id}
        Returns {item_id, total_quantity, reserved_quantity, available_quantity}.
    """
    inventory: dict[str, Any] = state.get("inventory", {})

    if query_name == "get_inventory":
        item_id = payload["item_id"]
        item = inventory.get(item_id)
        if item is None:
            raise KeyError(f"Item {item_id!r} not found")

        total = item["total_quantity"]
        reserved = sum(item["reservations"].values())
        return {
            "item_id": item_id,
            "total_quantity": total,
            "reserved_quantity": reserved,
            "available_quantity": total - reserved,
        }

    raise NotImplementedError(f"Unknown query: {query_name!r}")
