# Student Implementation README

## Sharding Design

### Partition Key

We selected `item_id` as the partition key for the inventory application.

This choice was based on the workload characteristics:
- All operations (create, reserve, release, get) are centered around a single item
- Keeping all state for a given item on the same shard avoids the need for cross-shard transactions
- This simplifies transaction execution and improves consistency

---

### Sharding Strategy

We used a **hash-based sharding strategy** to map partition keys to logical shards.

Implementation details:
- The partition key (`item_id`) is hashed using SHA-256
- The resulting hash value is converted to an integer
- The shard is determined using modulo:
  shard_id = hash(item_id) % total_logical_shards