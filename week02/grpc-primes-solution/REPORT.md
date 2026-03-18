# REPORT

## Goal
Convert the existing HTTP/JSON distributed prime computation system to a schema-based gRPC design.

## Implementation summary

1. Defined protobuf schema (`proto/primes.proto`) for:
   - WorkerService (`Health`, `ComputeRange`)
   - CoordinatorService (`Health`, `RegisterNode`, `ListNodes`, `Compute`)
   - request/response messages and enums for mode/exec

2. Replaced HTTP endpoints with gRPC RPCs:
   - Primary coordinator now receives worker registrations and compute requests over gRPC.
   - Secondary worker exposes compute RPC over gRPC.

3. Preserved original behavior:
   - mode: `count|list`
   - chunking
   - local execution model inside worker: `single|threads|processes`
   - capped returned prime list (`max_return_primes`) and truncation flag
   - optional per-node summary in distributed response

4. CLI compatibility:
   - local modes unchanged
   - distributed mode (`--exec distributed`) now calls gRPC coordinator
   - keeps `--primary` convenience flag

## Design choices
- Kept compute logic in reusable module (`grpc_common.py`) to avoid duplication.
- Coordinator fan-out uses thread pool to call workers concurrently.
- Worker heartbeat registration is periodic; coordinator expires stale nodes by TTL.

## Limitations / future improvements
- TLS and auth not enabled (demo/lab scope).
- Retry/backoff strategy for worker RPC fan-out can be enhanced.
- Better typed errors and richer status telemetry can be added.
