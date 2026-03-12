#!/usr/bin/env python3


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
from primes_in_range import primes_sieve


# ----------------------------
# Partitioning helpers
# ----------------------------

# BEGIN new

# generator version
def iter_ranges(low: int, high: int, chunk: int):
    """Yield contiguous chunks from [low, high) as a generator."""
    if chunk <= 0:
        raise ValueError("chunk must be > 0")

    x = low
    while x < high:
        y = min(x + chunk, high)
        yield (x, y)
        x = y

def _work_chunk(low: int, high: int, mode: int) -> primes_pb2.PerNodeResult:
    """
    Worker-side task: Sieves and packages data into a Protobuf message.
    """
    t0 = time.perf_counter()

    # 1. Call our validated "Pure Engine"
    seg = primes_sieve(low, high)

    # 2. Initialize the gRPC Message
    res = primes_pb2.PerNodeResult()
    res.slice.extend([low, high])

    # 3. Last Mile Processing (Still inside the worker process)
    if mode == primes_pb2.LIST:
        primes = [low + i for i, is_p in enumerate(seg) if is_p]
        res.primes.extend(primes)
        res.total_primes = len(primes)
        res.max_prime = primes[-1] if primes else -1
    else:
        res.total_primes = sum(seg)
        res.max_prime = -1 # Not calculated in COUNT mode for speed

    res.node_elapsed_s = time.perf_counter() - t0
    return res


# END

# BEGIN MACHINE SYMPATHETIC VERSION
import multiprocessing as mp
import time
import traceback
# from primes_in_range import worker_task  # Assumed top-level for pickling


# BEGIN # Minimal bytecode: unpack and delegate
def minimal_worker_task_wrapper(args):
    return worker_task(*args)
# END minimal wrapper

# BEGIN this one debug but also gives the actual rest of the fields
def worker_task_wrapper(args):
    low, high, mode = args
    print(f"DEBUG: [Worker] Starting range {low}-{high}")
    try:
        result = _work_chunk(low, high, mode)
        # Check if result is actually populated
        print(f"DEBUG: [Worker] {low}-{high} found {result.total_primes} primes.")
        return result
    except Exception as e:
        print(f"DEBUG: [Worker CRASH] {low}-{high}: {str(e)}")
        # Return something identifiable so the dispatcher knows it failed
        return f"ERROR: {str(e)}"
# END


# BEGIN real current version
def compute_partitioned(request):
  pool = None
  try:
    if request.secondary_exec in (primes_pb2.SINGLE, primes_pb2.THREADS):
      # Generate raw results for the ranges
      results = [
          _work_chunk(r[0], r[1], request.mode)
          for r in iter_ranges(
              request.low,
              request.high,
              request.chunk
          )
      ]
    else:
      workers = request.secondary_workers or mp.cpu_count()
      target_mode = request.mode
      results = []

      ctx = mp.get_context('spawn')
      pool = ctx.Pool(processes=workers)
      # pool = mp.Pool(processes=workers)

      task_generator = (
        (r[0], r[1], target_mode)
        for r in iter_ranges(
          request.low,
          request.high,
          request.chunk
        ) # END_FOR
      ) # END_TASK_GENERATOR

      try:
        for res in pool.imap_unordered(
          worker_task_wrapper,
          task_generator,
          chunksize=10
        ):
          results.append(res)
      finally:
        pool.terminate()
        pool.join()
#
    results.sort(key=lambda x: x.slice[0])
    response = primes_pb2.ComputeResponse()

    for r in results:
      response.total_primes += r.total_primes
      if r.max_prime > response.max_prime:
        response.max_prime = r.max_prime
      if request.mode == primes_pb2.LIST:
        current_len = len(response.primes)
        if current_len < request.max_return_primes:
          space_left = request.max_return_primes - current_len
          response.primes.extend(r.primes[:space_left])
          if len(r.primes) > space_left:
            response.primes_truncated = True
        else:
          response.primes_truncated = True

    return response
  # END_TRY
  except Exception as e:
    if pool:
      pool.terminate()
    error_msg = (
      f"Secondary Node internal error: {str(e)}\n"
      f"{traceback.format_exc()}"
    )
    print(error_msg)
    fail_res = primes_pb2.ComputeResponse()
    return fail_res
# END version

# END


# ----------------------------
# gRPC Worker Service
# ----------------------------

class WorkerService(primes_pb2_grpc.WorkerServiceServicer):
  """Implementing Task 2: WorkerService"""

  def ComputeRange(self, request, context):

    try:
      if request.high <= request.low:
        raise ValueError("high must be greater than low")
      if request.low < 2:
        raise ValueError("minimum low value is 2")
      return compute_partitioned(request)
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
    return primes_pb2. HealthCheckResponse(status="SERVING")

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

    mp.set_start_method('spawn', force=True)

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