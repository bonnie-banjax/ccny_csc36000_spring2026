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




# BEGIN temp enum double indireciton alias
from enum import IntEnum
import primes_pb2

# These provide IDE-friendly syntax highlighting and autocompletion
class PRIMES_MODE(IntEnum):
    COUNT = primes_pb2.COUNT
    LIST = primes_pb2.LIST

class EXEC_MODE(IntEnum):
    SINGLE = primes_pb2.SINGLE
    THREADS = primes_pb2.THREADS
    PROCESSES = primes_pb2.PROCESSES
# END


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

# BEGIN OLD / INTERMEDIATE REFACTOR
def compute_partitioned3432(
    low: int,
    high: int,
    *,
    mode: int = PRIMES_MODE.COUNT,
    chunk: int = 500_000,
    exec_mode: str = EXEC_MODE.SINGLE,
    workers: int | None = None,
    max_return_primes: int = 5000,
    include_per_chunk: bool = False,
) -> primes_pb2.ComputeResponse: # NOTE was Dict[str, Any]:

    # Resilience: Invalid input handling
    if high <= low:
        raise ValueError("high must be > low")

    if workers is None:
        workers = os.cpu_count() or 4
    workers = max(1, int(workers))

    response = primes_pb2.ComputeResponse() # initializes response object?

    ranges = iter_ranges(low, high, chunk)
    want_list = (mode == PRIMES_MODE.LIST) # NOTE changed from | mode == "list" |

    t0 = time.perf_counter()
    chunk_results: List[Dict[str, Any]] = [] # what does this become?


    # BEGIN still need to change how chunk result works
    match exec_mode:
      case EXEC_MODE.SINGLE:
        for a, b in ranges:
            chunk_results.append(_work_chunk((a, b, want_list)))
      case EXEC_MODE.THREADS:
        with futures.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in futures.as_completed(futs):
                chunk_results.append(f.result())
      case EXEC_MODE.PROCESSES:
        with futures.ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in futures.as_completed(futs):
                chunk_results.append(f.result())
      case _:
          raise ValueError("Unknown execution mode")
    # END

    t1 = time.perf_counter()

    chunk_results.sort(key=lambda d: int(d["low"]))
    total_primes = sum(int(d["prime_count"]) for d in chunk_results)

    max_prime = max((int(d["max_prime"]) for d in chunk_results), default=-1)
    primes_out = [] # TODO
    truncated = False

    # BEGIN bit unsure
    response.total_primes = sum(c.prime_count for c in chunk_results)
    response.max_prime = max((c.max_prime for c in chunk_results), default=-1)

    response.elapsed_seconds = t1 - t0
    # END BIT UNSURE

    # BEGIN new
    if want_list:
      for c in chunk_results:
        # Safely extend the 'repeated' field
        if len(response.primes) < max_return_primes:
          response.primes.extend(c.primes)
          # (Note: handle truncation logic here)
    # END new

    # BEGIN old
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
    # END old




    return response

    # TODO this defeats the entire point
    return {
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "primes": primes_out,
        "primes_truncated": truncated,
    }
# END

# BEGIN GENERATED REFACTOR (but over complicated)
def compute_partitioned333(request: primes_pb2.ComputeRequest) -> primes_pb2.ComputeResponse:

  if request.high <= request.low:
    raise ValueError("high must be > low")

  workers = request.secondary_workers or os.cpu_count() or 4

  ranges = iter_ranges_generator(request.low, request.high, request.chunk)

  # 3. Execution Strategy
  # Note: We pass the range and the mode into the worker
  results: List[primes_pb2.PerNodeResult] = []

  match request.secondary_exec:
    case primes_pb2.SINGLE:
      results = [_work_chunk(low, high, request.mode) for low, high in ranges]

    case primes_pb2.THREADS | primes_pb2.PROCESSES:
      Executor = (futures.ThreadPoolExecutor if request.secondary_exec == primes_pb2.THREADS
            else futures.ProcessPoolExecutor)

      with Executor(max_workers=workers) as ex:
        # We use a lambda or partial to freeze the 'mode' argument
        # map() pulls (low, high) from the ranges generator lazily
        func = functools.partial(_work_chunk, mode=request.mode)
        # starmap is used because ranges yields tuples of (low, high)
        results = list(ex.map(lambda r: func(*r), ranges))

  # 4. Aggregation (The "Collector" phase)
  response = primes_pb2.ComputeResponse()

  # Sort results by the 'low' value in the slice to ensure ordered primes
  results.sort(key=lambda r: r.slice[0])

  for r in results:
    response.total_primes += r.total_primes

    if r.max_prime > response.max_prime:
      response.max_prime = r.max_prime

    # Merge lists and handle truncation
    if request.mode == primes_pb2.LIST:
      if len(response.primes) < request.max_return_primes:
        space_left = request.max_return_primes - len(response.primes)
        response.primes.extend(r.primes[:space_left])
        if len(r.primes) > space_left:
          response.primes_truncated = True
      else:
        response.primes_truncated = True

  return response
# END

# BEGIN MACHINE SYMPATHETIC VERSION
import multiprocessing as mp
import time
import traceback
from primes_in_range import worker_task  # Assumed top-level for pickling



# BEGIN commented version
def compute_partitioned3(request):
  try:
    # 1. Immediate Dispatch
    # We bypass the 'match' and 'class assignment' to avoid bytecode overhead.
    # If the user asked for SINGLE, we don't even touch the multiprocessing logic.
    if request.secondary_exec == 0: # Assuming 0 is SINGLE
      return [_work_chunk(r[0], r[1], request.mode) for r in iter_ranges(request.low, request.high, request.chunk)]

    # 2. Processor Affinity & Pool Management
    # We use the lower-level mp.Pool which is often faster than ProcessPoolExecutor
    # because it has less management overhead per task.
    workers = request.secondary_workers or mp.cpu_count()

    # Pre-calculating the static mode so it doesn't have to be resolved per-loop
    target_mode = request.mode

    # 3. Minimizing IPC (Inter-Process Communication)
    # Instead of map(), we use imap_unordered.
    # CPython's imap_unordered allows the main thread to begin aggregating
    # the MOMENT the first worker finishes, rather than waiting for
    # the generator to be fully consumed or tasks to finish in order.

    results = []
    pool = mp.Pool(processes=workers)

    # We feed a generator of 3-tuples. This is a single 'pickle' operation per task.
    task_generator = ((r[0], r[1], target_mode) for r in iter_ranges(request.low, request.high, request.chunk))

    try:
      # chunksize=10 is a 'mechanical' optimization.
      # It sends 10 ranges at a time to a worker process, reducing the
      # number of times the OS has to perform a context switch for IPC.
      for res in pool.imap_unordered(worker_task_wrapper, task_generator, chunksize=10):
        results.append(res)
    finally:
      pool.terminate()
      pool.join()

    # 4. Final Aggregation
    # Since imap_unordered destroyed our order, we sort once at the end.
    # Sorting a list of references in CPython is highly optimized C code (Timsort).

      results.sort(key=lambda x: x.slice[0])

      # Initialize the container. This is a heap allocation for the Protobuf message.
      response = primes_pb2.ComputeResponse()

      # If results is empty, response returns with default values (0, -1, etc.)
    for r in results:
      # Accumulate the scalar total
      response.total_primes += r.total_primes

      # Update the high-water mark for primes
      if r.max_prime > response.max_prime:
        response.max_prime = r.max_prime

      # List Merging Logic
      if request.mode == primes_pb2.LIST:
        current_len = len(response.primes)
        if current_len < request.max_return_primes:
          space_left = request.max_return_primes - current_len
          # We extend the repeated field using the worker's data
          response.primes.extend(r.primes[:space_left])

          # If the worker had more than we could take, flag truncation
          if len(r.primes) > space_left:
            response.primes_truncated = True
        else:
          # We are already full, so any further results imply truncation
          response.primes_truncated = True

    return response
  except Exception as e:
    # Mechanical Sympathy: Log the full traceback so the dev knows WHY it died
    error_msg = f"Secondary Node internal error: {str(e)}\n{traceback.format_exc()}"
    print(error_msg)

    # Create a failure response instead of crashing the process
    fail_res = primes_pb2.ComputeResponse()
    # Assuming your .proto has an 'error_message' or 'status' field
    # fail_res.error_message = error_msg
    # fail_res.status = primes_pb2.FAILED

    return fail_res
# END commented version

# BEGIN real version
def compute_partitioned_whoops(request):
  pool = None

  try:
    # if (
    #   (request.secondary_exec == primes_pb2.SINGLE) or
    #   (request.secondary_exec == primes_pb2.THREADS)
    # ):
    if request.secondary_exec in (primes_pb2.SINGLE, primes_pb2.THREADS):
      return [
        _work_chunk(r[0], r[1], request.mode)
        for r in iter_ranges_generator(
          request.low,
          request.high,
          request.chunk
        )
      ]

    workers = request.secondary_workers or mp.cpu_count()
    target_mode = request.mode
    results = []
    pool = mp.Pool(processes=workers)
    task_generator = (
      (r[0], r[1], target_mode)
      for r in iter_ranges_generator(
        request.low,
        request.high,
        request.chunk
      )
    )
    # with mp.Pool(processes=workers) as pool:
    #   task_generator = (
    #     (r[0], r[1], target_mode)
    #     for r in iter_ranges_generator(
    #       request.low,
    #       request.high,
    #       request.chunk
    #     )
    #   )

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

  except Exception as e:
    if pool:
      pool.terminate()
    error_msg = (
      f"Secondary Node internal error: {str(e)}\n"
      f"{traceback.format_exc()}"
    )
    # error_msg = (f"Secondary Node internal error: {str(e)}\n")
    print(error_msg)
    fail_res = primes_pb2.ComputeResponse()
    return fail_res
# END version


# def worker_task_wrapper(args):
#     # Minimal bytecode: unpack and delegate
#     return worker_task(*args)

# BEGIN lmao forgot this one above doesn't include ma
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

# BEGIN debug code
    # # Before the loop
    # print(f"DEBUG: Dispatching tasks with {workers} workers.")
    #
    # try:
    #   for res in pool.imap_unordered(
    #     worker_task_wrapper,
    #     task_generator,
    #     chunksize=10
    #   ):
    #     # Print the raw type and content of the result
    #     print(f"DEBUG: Received result type: {type(res)}")
    #     if res:
    #       print(f"DEBUG: res.total_primes = {getattr(res, 'total_primes', 'N/A')}")
    #     else:
    #       print("DEBUG: Received None from worker!")
    #     results.append(res)
    # finally:
    #   print(f"DEBUG: Pool terminated. Total results collected: {len(results)}")
    #   pool.terminate()
    #   pool.join()
# END

# BEGIN real version
def compute_partitioned(request):
  pool = None
  try:
    if request.secondary_exec in (primes_pb2.SINGLE, primes_pb2.THREADS):
      return [
        _work_chunk(r[0], r[1], request.mode)
        for r in iter_ranges_generator(
          request.low,
          request.high,
          request.chunk
        ) # END_FOR
      ] # END_RETRUN

    workers = request.secondary_workers or mp.cpu_count()
    target_mode = request.mode
    results = []
    pool = mp.Pool(processes=workers)
    task_generator = (
      (r[0], r[1], target_mode)
      for r in iter_ranges_generator(
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
      return compute_partitioned(request)
      # BEGIN this is (or should be) redundant now, yes?
      res = compute_partitioned(
        low=request.low,
        high=request.high,
        mode=request.mode,
        chunk=request.chunk,
        exec_mode=request.secondary_exec,
        workers=request.secondary_workers,
        max_return_primes=request.max_return_primes,
        include_per_chunk=request.include_per_node
      )
      # END redundent intermediate call
      # Returning typed protobuf responses
      return primes_pb2.ComputeResponse(
        total_primes=res.total_primes,
        max_prime=res.max_prime,
        elapsed_seconds=res.elapsed_seconds,
        primes=res.primes,
        primes_truncated=res.primes_truncated
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