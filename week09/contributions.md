## Tasnuva Chowdhury

- Implemented the sharding layer in `student_impl/sharding.py`
  - Designed and selected `item_id` as the partition key for the inventory application
  - Implemented stable shard mapping using a hash-based approach (`SHA-256`)
  - Ensured deterministic routing so that the same item always maps to the same logical shard

- Verified sharding correctness through testing
  - Created local tests to confirm stability (same key → same shard)
  - Performed distribution testing to ensure keys are reasonably balanced across shards

- Contributed to end-to-end validation
  - Verified routing consistency through client commands and cluster responses
  - Ensured that sharding integrates correctly with the transaction and storage layers