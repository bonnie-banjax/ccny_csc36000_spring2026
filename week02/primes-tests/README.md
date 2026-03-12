# Prime Tests (Week 02)

These tests validate the gRPC prime-service functionality described in `week02/README.md`.

## Location

- Tests live in `kbrown6/week02/primes-tests`.
- By default, they target `kbrown6/week02/grpc-primes-solution`.

## Run

From repository root:

```bash
pytest kbrown6/week02/primes-tests
```

To point tests at a different implementation directory:

```bash
pytest kbrown6/week02/primes-tests --impl-dir kbrown6/week02/<your-implementation-dir>
```

`--impl-dir` can be either:

- The exact implementation directory.
- A parent directory that contains exactly one matching implementation subtree (week01-style project layouts).

The resolved implementation directory must contain:

- `primary_node.py`
- `secondary_node.py`
- `grpc_common.py`
- `primes_in_range.py`
- optional `generated/` (for protobuf imports)
