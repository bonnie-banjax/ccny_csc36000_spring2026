#!/usr/bin/env python3
"""
secondary_node.py

A "secondary node" HTTP server that exposes prime-range computation via HTTP.

Key features
------------
- Exposes POST /compute with the same partitioning + thread/process execution model as primes_cli.py
- On startup, optionally registers itself with a primary coordinator (primary_node.py) via POST /register
  so the primary can discover and distribute work across all secondary nodes.

Endpoints
---------
GET  /health
    -> {"ok": true, "status": "healthy"}

GET  /info
    -> basic node metadata (host/port/node_id/cpu_count)

POST /compute
    JSON body:
    {
      "low": 0,                   (required)
      "high": 1000000,            (required; exclusive)
      "mode": "count"|"list",     default "count"
      "chunk": 500000,            default 500000
      "exec": "single"|"threads"|"processes", default "single"
      "workers": 8,               default cpu_count
      "max_return_primes": 5000,  default 5000 (only used when mode="list")
      "include_per_chunk": true   default false (summary only; avoids huge responses)
    }

Notes
-----
- Example of how to run from terminal: python3 week01/secondary_node.py --primary http://127.0.0.1:9200 --node-id kbrown
- For classroom demos, use mode="count" for big ranges to avoid large payloads.
- "threads" may not speed up CPU-bound work in CPython; "processes" usually will.
"""

from __future__ import annotations

import argparse
import os
import socket
import time
import grpc
from concurrent import futures 
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse

import primes_pb2
import primes_pb2_grpc
from primes_in_range import get_primes

# ----------------------------
# Partitioning helpers
# ----------------------------

def iter_ranges(low: int, high: int, chunk: int) -> List[Tuple[int, int]]:
    """Split [low, high) into contiguous chunks."""
    if chunk <= 0:
        raise ValueError("chunk must be > 0")
    out: List[Tuple[int, int]] = []
    x = low
    while x < high:
        y = min(x + chunk, high)
        out.append((x, y))
        x = y
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Dict[str, Any]:
    """
    Worker for one chunk.
    Returns dict for easy JSON serialization.
    """
    low, high, return_list = args

    t0 = time.perf_counter()
    res = get_primes(low, high, return_list=return_list)
    t1 = time.perf_counter()

    if return_list:
        primes = list(res)  # type: ignore[arg-type]
        return {
            "low": low,
            "high": high,
            "elapsed_s": t1 - t0,
            "prime_count": len(primes),
            "max_prime": primes[-1] if primes else -1,
            "primes": primes,
        }

    count = int(res)  # type: ignore[arg-type]
    return {
        "low": low,
        "high": high,
        "elapsed_s": t1 - t0,
        "prime_count": count,
        "max_prime": -1,  # not computed in count mode to avoid extra work
    }


def compute_partitioned(
    low: int,
    high: int,
    *,
    mode: str = "count",
    chunk: int = 500_000,
    exec_mode: str = "single",
    workers: int | None = None,
    max_return_primes: int = 5000,
    include_per_chunk: bool = False,
) -> Dict[str, Any]:
    """
    Perform partitioned computation over [low, high) using get_primes per chunk.
    """
    # Resilience: Invalid input handling
    if high <= low:
        raise ValueError("high must be > low")
    
    if workers is None:
        workers = os.cpu_count() or 4
    workers = max(1, int(workers))

    ranges = iter_ranges(low, high, chunk)
    want_list = (mode == "list")

    t0 = time.perf_counter()
    chunk_results: List[Dict[str, Any]] = []

    # Parallel and Single execution paths
    if exec_mode == "single":
        for a, b in ranges:
            chunk_results.append(_work_chunk((a, b, want_list)))
    elif exec_mode == "threads":
        with futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in futures.as_completed(futs):
                chunk_results.append(f.result())
    else:  # processes
        with futures.ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in futures.as_completed(futs):
                chunk_results.append(f.result())

    t1 = time.perf_counter()

    chunk_results.sort(key=lambda d: int(d["low"]))
    total_primes = sum(int(d["prime_count"]) for d in chunk_results)

    max_prime = max((int(d["max_prime"]) for d in chunk_results), default=-1)
    primes_out = []
    truncated = False
    
    if want_list:
        for d in chunk_results:
            ps = d.get("primes") or []
            if len(primes_out) < max_return_primes:
                remaining = max_return_primes - len(primes_out)
                primes_out.extend(ps[:remaining])
                if len(ps) > remaining: 
                    truncated = True
            else: 
                truncated = True

    # Return data mapped for gRPC WorkerService
    return {
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "primes": primes_out,
        "primes_truncated": truncated,
    }

# ----------------------------
# gRPC Worker Service 
# ----------------------------

class WorkerService(primes_pb2_grpc.WorkerServiceServicer):
    """Implementing Task 2: WorkerService"""

    def ComputeRange(self, request, context):
        # Mapping Enums (Task 1) to Business Logic strings
        mode_str = "count" if request.mode == primes_pb2.COUNT else "list"
        exec_map = {
            primes_pb2.SINGLE: "single",
            primes_pb2.THREADS: "threads",
            primes_pb2.PROCESSES: "processes"
        }

        try:
            res = compute_partitioned(
                low=request.low, high=request.high, mode=mode_str,
                chunk=request.chunk, exec_mode=exec_map.get(request.secondary_exec, "single"),
                workers=request.secondary_workers, max_return_primes=request.max_return_primes,
                include_per_chunk=request.include_per_node
            )
            # Returning typed protobuf responses
            return primes_pb2.ComputeResponse(
                total_primes=res["total_primes"],
                max_prime=res["max_prime"],
                elapsed_seconds=res["elapsed_seconds"],
                primes=res["primes"],
                primes_truncated=res["primes_truncated"]
            )
        except ValueError as e:
            # Resilience: Error mapping for invalid inputs
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return primes_pb2.ComputeResponse()
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Internal error: {str(e)}")
            return primes_pb2.ComputeResponse()

    def Health(self, request, context):
        """Standard gRPC Health check"""
        return primes_pb2.HealthCheckResponse(status="SERVING")

# ----------------------------
# Helper: IP Detection
# ----------------------------

def _guess_local_ip_for(primary_host: str) -> str:
    """Detect local IP to advertise during gRPC registration"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((primary_host, 50051)) # arbitrary port
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


# ----------------------------
# Refactored Main Function
# ----------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Secondary gRPC worker node.")
    ap.add_argument("--host", default="[::]", help="Bind host.")
    ap.add_argument("--port", type=int, default=50052, help="Bind port.")
    ap.add_argument("--node-id", default=socket.gethostname(), help="Node ID.")
    ap.add_argument("--coordinator", default="localhost:50051", help="Coordinator address.")
    args = ap.parse_args()

    # Step 1: Start the gRPC Server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    primes_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    server.add_insecure_port(f"{args.host}:{args.port}")
    server.start()
    
    print(f"[worker] node_id={args.node_id} listening on {args.port}")

    # Step 2: Startup Registration using gRPC
    if args.coordinator:
        try:
            channel = grpc.insecure_channel(args.coordinator)
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            
            adv_host = _guess_local_ip_for(args.coordinator.split(':')[0])
            reg_request = primes_pb2.RegisterRequest(
                node_id=args.node_id,
                host=adv_host,
                port=args.port
            )
            # Synchronous registration call on startup
            stub.RegisterNode(reg_request)
            print(f"[worker] Registered to coordinator at {args.coordinator}")
        except Exception as e:
            print(f"[worker] Registration failed: {e}")

    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(0)

if __name__ == "__main__":
    main()