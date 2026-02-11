# TESTING

## Unit tests
- `tests/test_chunking.py`
- `tests/test_partitioned_compute.py`

## Integration test
- `tests/test_grpc_integration.py` starts coordinator and worker on ephemeral ports,
  registers worker, runs a distributed COUNT request, and validates result correctness.

## Run
```bash
make test
```

Expected: all tests pass.
