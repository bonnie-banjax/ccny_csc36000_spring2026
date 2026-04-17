from __future__ import annotations

import hashlib
from typing import Any


TOTAL_LOGICAL_SHARDS = 8


def build_partition_key(application_name: str, operation_name: str, payload: dict[str, Any]) -> str:
    """
    Return the partition key used to route a request.

    For the inventory application every operation is scoped to a single
    item_id, so we use that as the partition key.  This means all
    reservations for a given item always land on the same logical shard,
    making every transaction single-shard and avoiding cross-shard
    coordination entirely.
    """
    return str(payload.get("item_id", ""))


def choose_logical_shard(partition_key: str, total_logical_shards: int = TOTAL_LOGICAL_SHARDS) -> int:
    """
    Map a partition key to a logical shard id in the range [0, total_logical_shards).

    Uses the lower 64 bits of an MD5 digest so keys spread evenly across
    shards (HASH_DISTRIBUTED tradeoff).
    """
    digest = int(hashlib.md5(partition_key.encode()).hexdigest(), 16)
    return digest % total_logical_shards
