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


## Andrew Mobus

- Transactions:
  - proof of correctness & understanding model (`docs/transaction-visualizer.html`)
  - initial transaction implementation (`student_impl/transactions.py`) re:
    - Concurrency Control via thread-locking mutex (SHARD_LOCK)
    - Read Isolation & Durability through atomic flush to disk

- Integration & Testing:
  - validation & iterative redevelopment of initial transaction logic
  - routine integrations testing to validate final merge stages

- Git Janitor:
  - ensured team followed proper git best practices
  - implemented merge controls & pull request reviews on main


## 
