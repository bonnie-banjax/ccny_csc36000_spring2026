from __future__ import annotations

import hashlib
from typing import Any


TOTAL_LOGICAL_SHARDS = 8


def build_partition_key(application_name: str, operation_name: str, payload: dict[str, Any]) -> str:
    """
    Return the partition key used to route a request.

    Students should implement a partition-key choice that matches the
    selected application's workload and explain the tradeoffs in
    student_impl/README.md.

    For the inventory application, shard by item_id so that all data for one
    inventory item stays together on one logical shard.
    """
    if application_name != "inventory":
        raise NotImplementedError(
            f"build_partition_key() currently supports only inventory, got {application_name!r}"
        )

    item_id = payload.get("item_id")
    if not isinstance(item_id, str) or not item_id:
        raise ValueError("inventory requests must include a non-empty string item_id")

    return item_id


def choose_logical_shard(partition_key: str, total_logical_shards: int = TOTAL_LOGICAL_SHARDS) -> int:
    """
    Map a partition key to a logical shard id in the range [0, total_logical_shards).

    The tests will check that your sharding function spreads a representative
    set of keys relatively evenly across the available logical shards.

    Using a stable hash so the same key always maps to the same shard across 
    restarts and across different Python processes.
    """
    if not isinstance(partition_key, str) or not partition_key:
        raise ValueError("partition_key must be a non-empty string")

    if total_logical_shards <= 0:
        raise ValueError("total_logical_shards must be positive")

    digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
    hash_value = int.from_bytes(digest[:8], byteorder="big", signed=False)
    return hash_value % total_logical_shards
