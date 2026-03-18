# gRPC Prime Tests

These tests validate the week02 requirements from `kbrown6/week02/README.md`.

## Run

```bash
pytest kbrown6/week02/primes-tests --impl-dir <path-to-week02-implementation>
```

Example:

```bash
pytest kbrown6/week02/primes-tests --impl-dir group7/week02
```

Optional TTL startup flag:

```bash
pytest kbrown6/week02/primes-tests --impl-dir group7/week02 --pass-ttl
```

Default behavior: `--pass-ttl` is `False`, so tests do not pass `--ttl` when starting primary/secondary.

## Rubric Grading (100 pts)

Run rubric grading for one or more project directories:

```bash
python kbrown6/week02/primes-tests/grade_projects.py group1/week02 group7/week02
```

To enable TTL passing during grading:

```bash
python kbrown6/week02/primes-tests/grade_projects.py group1/week02 --pass-ttl
```

Category weights:
- `schema`: 20
- `correctness`: 25
- `grpc_impl`: 20
- `resilience`: 15
- `testing_repro`: 10
- `documentation`: 10
