# gRPC Primes — Full Solution

This is the **completed solution** for migrating the HTTP/JSON prime-distribution project to **schema-based gRPC**.

## What was converted

- `primary_node.py` → gRPC **CoordinatorService**
- `secondary_node.py` → gRPC **WorkerService**
- `primes_cli.py` distributed mode now calls Coordinator via gRPC
- shared contract defined in `proto/primes.proto`
- generated stubs in `generated/`

Local execution (`single|threads|processes`) is preserved in `primes_cli.py`.

---

## Setup

```bash
make install
```

---

## Run (3 terminals)

### Terminal 1 — Coordinator
```bash
make run-primary
```

### Terminal 2 — Worker
```bash
make run-worker
```

### Terminal 3 — Client
```bash
make cli-count
# or
make cli-list
```

---

## Compatibility notes

To preserve old CLI habits, distributed mode still accepts:

```bash
--primary 127.0.0.1:50051
# or
--primary http://127.0.0.1:50051
```

---

## Important files

- `proto/primes.proto` — schema/contract
- `primary_node.py` — coordinator fan-out + aggregation
- `secondary_node.py` — worker compute service + registration heartbeat
- `primes_cli.py` — local & distributed client CLI
- `grpc_common.py` — shared chunking and local compute engine

---

## Tests

```bash
make test
```

See `TESTING.md` for expected behavior.
