#!/usr/bin/env python3

from __future__ import annotations
import argparse
import os
import sys
import time
import urllib.request

from typing import List, Tuple

import grpc
import primes_pb2
import primes_pb2_grpc


# def _grpc_compute(target: str, request: primes_pb2.ComputeRequest) -> primes_pb2.ComputeResponse:
#     """A clean, sympathetic network call. No dictionary translation."""
#     with grpc.insecure_channel(target) as channel:
#         stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
#         return stub.Compute(request, timeout=3600)

#  BEGIN
def build_compute_request(args: argparse.Namespace) -> primes_pb2.ComputeRequest:
    """
    Pure transformation: Namespace -> Protobuf Message.
    Zero side effects. Zero networking.
    """
    # Map string choices to Protobuf Enum values
    # Assuming your .proto defines Mode { COUNT=0; LIST=1; }
    # and Exec { SINGLE=0; THREADS=1; PROCESSES=2; }

    mode_map = {"count": 0, "list": 1}
    exec_map = {"single": 0, "threads": 1, "processes": 2}

    return primes_pb2.ComputeRequest(
        low=args.low,
        high=args.high,
        mode=mode_map.get(args.mode, 0),
        chunk=args.chunk,
        secondary_exec=exec_map.get(args.secondary_exec, 2),
        secondary_workers=args.secondary_workers or 0,
        max_return_primes=args.max_return_primes,
        include_per_node=args.include_per_node
    )
# END


def _grpc_compute(primary: str, args) -> primes_pb2.ComputeResponse:
    try:
        target = primary
        with grpc.insecure_channel(target) as channel:
            stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
            req = build_compute_request(args)
            resp = stub.Compute(req, timeout=3600)
            return resp
    except Exception as e:
        print(f"_grpc_compute error: {e}", file=sys.stderr)
        return primes_pb2.ComputeResponse()



def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Prime counting/listing over [low, high) using local threads/processes OR distributed secondary nodes."
    )
    ap.add_argument("--low", type=int, required=True, help="Range start (inclusive).")
    ap.add_argument("--high", type=int, required=True, help="Range end (exclusive). Must be > low.")
    ap.add_argument("--mode", choices=["list", "count"], default="count")
    ap.add_argument("--chunk", type=int, default=500_000)
    ap.add_argument("--exec", choices=["single", "threads", "processes", "distributed"], default="single")
    ap.add_argument("--workers", type=int, default=(os.cpu_count() or 4))
    ap.add_argument("--max-print", type=int, default=50)
    ap.add_argument("--time", action="store_true")

    # Distributed options
    ap.add_argument("--primary", default=None, help="Primary URL, e.g. http://134.74.160.1:9200")
    ap.add_argument("--secondary-exec", choices=["single", "threads", "processes"], default="processes")
    ap.add_argument("--secondary-workers", type=int, default=None)
    ap.add_argument("--include-per-node", action="store_true")
    ap.add_argument("--max-return-primes", type=int, default=5000)

    args = ap.parse_args(argv)

    if args.high <= args.low:
        print("Error: --high must be > --low", file=sys.stderr)
        return 2

    return_list = (args.mode == "list")

    if args.exec == "distributed":
        if not args.primary:
            print("Error: --primary is required when --exec distributed", file=sys.stderr)
            return 2

        t0 = time.perf_counter()
        resp = _grpc_compute(args.primary, args)
        t1 = time.perf_counter()

# BEGIN ### ################################################################ ###
#NOTE this is almost entirely unchecked (generated), but it's "just" reporting #
        # if resp.total_primes == 0:
        #     print(f"Distributed error: {resp}", file=sys.stderr)
        #     return 1
        #
        # if resp.get("partial_failure"):
        #     print("\n[!] WARNING: Partial result received. Some nodes failed.", file=sys.stderr)
        #     if "failed_slices" in resp:
        #         for fail in resp["failed_slices"]:
        #             print(f"    - Node {fail['node_id']} failed range {fail['slice']}: {fail['error']}", file=sys.stderr)
        #     print("-" * 40, file=sys.stderr)
        #
        # if args.mode == "count":
        #     suffix = " (PARTIAL)" if resp.get("partial_failure") else ""
        #     print(f"{int(resp.get('total_primes', 0))}{suffix}")
        # else:
        #     primes = list(resp.get("primes", []))
        #     total = int(resp.get("total_primes", len(primes)))
        #     shown = primes[: args.max_print]
        #     print(f"Total primes: {total}")
        #     print(f"First {len(shown)} primes (from returned sample):")
        #     print(" ".join(map(str, shown)))
        #     if resp.primes_truncated or total > len(primes):
        #         print(f"... (returned primes are capped at {resp.get('max_return_primes', args.max_return_primes)})")
#NOTE: ie, improves correctness rather than robustness, per se... TODO         #
# END   ### ################################################################ ###

# BEGIN
# 1. Partial Failure Check
        # Protobuf doesn't have .get(). If 'partial_failure' isn't in your .proto yet,
        # this will raise an AttributeError. I've wrapped it in a hasattr check for now.

        is_partial = False
        if hasattr(resp, "partial_failure") and resp.partial_failure:
            is_partial = True
            print("\n[!] WARNING: Partial result received. Some nodes failed.", file=sys.stderr)

            # Use 'if resp.failed_slices:' to check if a repeated field has entries
            if hasattr(resp, "failed_slices") and resp.failed_slices:
                for fail in resp.failed_slices:
                    print(f"    - Node {fail.node_id} failed range {fail.slice}: {fail.error}", file=sys.stderr)
            print("-" * 40, file=sys.stderr)
        """
        Docstring Suggestion:
        Once 'partial_failure' and 'failed_slices' are stabilized in the .proto,
        remove the 'hasattr' checks to let the type system enforce correctness.
        """

        # 2. Main Result Reporting
        if args.mode == "count":
            suffix = " (PARTIAL)" if is_partial else ""
            # Accessing attribute directly: resp.total_primes
            print(f"{resp.total_primes}{suffix}")

        else:
            # resp.primes is a repeated field (list-like)
            primes_list = list(resp.primes)
            total = resp.total_primes
            shown = primes_list[: args.max_print]

            print(f"Total primes: {total}")
            print(f"First {len(shown)} primes (from returned sample):")
            print(" ".join(map(str, shown)))

            # Check for truncation
            # Note: total > len(primes_list) is the most reliable way to detect truncation
            if resp.primes_truncated or total > len(primes_list):
                # Use args.max_return_primes as the fallback label
                limit_val = getattr(resp, "max_return_primes", args.max_return_primes)
                print(f"... (returned primes are capped at {limit_val})")

        # 3. Performance Summary
        if args.time:
            # secondary_exec is an ENUM (int).
            # If you want the string name, you use: primes_pb2.Mode.Name(resp.secondary_exec)
            exec_name = args.secondary_exec # Fallback to CLI arg name for readability
            print(
                f"Elapsed seconds: {t1 - t0:.6f}  "
                f"(exec=distributed, nodes_used={resp.nodes_used}, "
                f"secondary_exec={exec_name}, chunk={args.chunk})",
                file=sys.stderr,
            )

# END


# BEGIN
        if args.time:
            print(
                f"Elapsed seconds: {t1 - t0:.6f}  "
                f"(exec=distributed, nodes_used={resp.nodes_used}, secondary_exec={resp.secondary_exec}, chunk={args.chunk})",
                file=sys.stderr,
            )
            if args.include_per_node and per_node in resp:               #TODO
                print("Per-node summary:", file=sys.stderr)
                for r in resp.per_node:
                    print(
                        f"  {r.node_id:>12} slice={r.slice} primes={r.total_primes} "
                        f"node_elapsed={r.node_elapsed_s:.3f}s round_trip={r.round_trip_s:.3f}s",
                        file=sys.stderr,
                    )
# END
        return 0

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
