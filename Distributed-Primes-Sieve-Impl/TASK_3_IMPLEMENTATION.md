# Task 3: CoordinatorService Implementation

## Overview

Task 3 converts the primary node from HTTP-only to a hybrid gRPC + HTTP coordinator that manages distributed prime computation across worker nodes.

## Implementation Details

### RegisterNode RPC

```python
def RegisterNode(self, request: primes_pb2.RegisterRequest, context):
    node = {
        "node_id": request.node_id,
        "host": request.host,
        "port": request.port,
    }
    REGISTRY.upsert(node)
    return primes_pb2.RegisterResponse(success=True)
```

Workers call this on startup to register themselves. Registry keeps nodes alive with TTL (default 120s). Health check probes remove stale nodes.

### ListNodes RPC

```python
def ListNodes(self, request, context) -> primes_pb2.ListNodesResponse:
    nodes = REGISTRY.active_nodes()
    resp = primes_pb2.ListNodesResponse()
    for node in nodes:
        node_info = resp.nodes.add()
        node_info.node_id = node["node_id"]
        node_info.host = node["host"]
        node_info.port = node["port"]
    return resp
```

Allows clients to query active workers. Automatically expires nodes beyond TTL. Returns structured NodeInfo messages.

### Compute RPC with Fanout

```python
def Compute(self, request: primes_pb2.ComputeRequest, context):
    slices = split_into_slices(request.low, request.high, len(nodes_sorted))
    
    with ThreadPoolExecutor(...) as ex:
        for node, slice in zip(nodes_sorted, slices):
            ex.submit(call_worker_grpc, node, slice)
```

Splits range [low, high) into N slices for N workers. Fans out to each worker via gRPC concurrently using ThreadPoolExecutor. Aggregates results from all workers and sums primes. Partial failures are handled gracefully.

### Timeouts on Worker RPCs

```python
def call_worker_grpc(node, slice):
    stub = primes_pb2_grpc.WorkerStub(channel)
    worker_resp = stub.ComputeRange(worker_req, timeout=3600)
```

Each worker RPC has 3600 second timeout. Prevents hung coordinator from blocking indefinitely. Exceeded deadlines trigger graceful error handling.

### Error Mapping

```python
except Exception as e:
    context.set_details(f"Compute failed: {e}")
    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
    raise
```

Error handling:
- INVALID_ARGUMENT: high <= low, no nodes, bad mode
- UNAVAILABLE: All nodes down, timeouts
- INTERNAL: Unexpected failures
- Worker errors tracked per-node in response

## Schema Updates

### New Messages

```protobuf
message NodeInfo {
  string node_id = 1;
  string host = 2;
  int32 port = 3;
}

message ListNodesRequest {}

message ListNodesResponse {
  repeated NodeInfo nodes = 1;
}
```

### Updated CoordinatorService

```protobuf
service CoordinatorService {
  rpc RegisterNode(RegisterRequest) returns (RegisterResponse);
  rpc ListNodes(ListNodesRequest) returns (ListNodesResponse);
  rpc Compute(ComputeRequest) returns (ComputeResponse);
}
```

## Architecture

### Coordinator Registry
- In-memory dictionary mapping node_id to node metadata
- TTL expiration: Nodes removed if not seen for 120 seconds
- Thread-safe: Lock-protected during probes and fanout

### Health Probing
- Before fanout, coordinator probes each node's /health endpoint
- Removes dead nodes from working set
- Ensures fanout targets are alive

### Fanout Pattern
```
Primary → [Node A: slice 0..5000]
       ├→ [Node B: slice 5000..10000]
       └→ [Node C: slice 10000..15000]
```
- Parallel execution with ThreadPoolExecutor (max_workers=32)
- Per-worker results track elapsed time, primes found, errors
- Aggregation sums total_primes and tracks partial_failure

### Failure Handling

| Scenario | Handling |
|----------|----------|
| 1 of 3 workers fails | partial_failure=True, return successful results + error details |
| All workers fail | StatusCode.UNAVAILABLE error to client |
| Worker timeout | Deadline exceeded, treated as node failure |
| Invalid range | StatusCode.INVALID_ARGUMENT before fanout |
| No nodes registered | StatusCode.UNAVAILABLE error |

## Startup

```bash
# Terminal 1: Start Coordinator (gRPC on 9201, HTTP on 9200)
python week01/core/primary_node.py --host 127.0.0.1 --port 9200 --grpc-port 9201

# Terminal 2,3: Start Workers (register themselves via gRPC)
python week01/core/secondary_node.py --primary-host 127.0.0.1 --primary-grpc-port 9201

# Terminal 4: Client call
python week01/core/primes_cli.py --low 0 --high 100000 --exec distributed --primary http://127.0.0.1:9200
```

## Files Modified

| File | Changes |
|------|---------|
| week01/primes.proto | Added NodeInfo, ListNodesRequest/Response, updated CoordinatorService RPC list |
| week01/core/primes_pb2.py | Regenerated (ListNodes messages) |
| week01/core/primes_pb2_grpc.py | Regenerated (ListNodes RPC stub) |
| week01/core/primary_node.py | Added CoordinatorServicer class, gRPC server in main(), worker fanout via gRPC |
