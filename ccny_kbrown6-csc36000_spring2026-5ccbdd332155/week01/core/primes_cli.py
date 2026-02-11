#!/usr/bin/env python3
"""
primes_cli.py

Notes
-----
- Examples of how to run from terminal:
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec single --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec threads --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec processes --time --mode count
python3 week01/primes_cli.py --low 0 --high 100_000_0000 --exec distributed --time --mode count --secondary-exec processes --primary http://127.0.0.1:9200
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import List, Tuple
from primes_in_range import get_primes

try:
    import grpc
    import primes_pb2
    import primes_pb2_grpc
    GRPC_AVAILABLE = True
except ImportError:
    GRPC_AVAILABLE = False


def iter_ranges(low: int, high: int, chunk: int) -> List[Tuple[int, int]]:
    """Split [low, high) into contiguous chunks."""
    if chunk <= 0:
        raise ValueError("--chunk must be > 0")
    out: List[Tuple[int, int]] = []
    x = low
    while x < high:
        y = min(x + chunk, high)
        out.append((x, y))
        x = y
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Tuple[int, int, object]:
    a, b, return_list = args
    res = get_primes(a, b, return_list=return_list)
    return (a, b, res)


def _post_json(url: str, payload: dict, timeout_s: int = 3600) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _grpc_compute(primary: str, args, return_list: bool) -> dict:
    """Call Compute via gRPC. Falls back to HTTP if gRPC unavailable or errors."""
    if not GRPC_AVAILABLE:
        return {"ok": False, "error": "gRPC not available; using HTTP"}
    
    try:
        # Parse primary address (strip http:// if present)
        target = primary
        if target.startswith("http://"):
            target = target[7:]
        if target.startswith("https://"):
            target = target[8:]
        target = target.rstrip("/")
        if ":" not in target:
            target = f"{target}:9201"  # Default gRPC port
        
        channel = grpc.insecure_channel(target)
        stub = primes_pb2_grpc.CoordinatorStub(channel)
        
        mode_val = primes_pb2.Mode.LIST if return_list else primes_pb2.Mode.COUNT
        exec_map = {"single": 0, "threads": 1, "processes": 2}
        exec_val = exec_map.get(args.secondary_exec, 2)
        
        req = primes_pb2.ComputeRequest(
            low=args.low,
            high=args.high,
            mode=mode_val,
            chunk=args.chunk,
            secondary_exec=exec_val,
            secondary_workers=args.secondary_workers or 0,
            max_return_primes=args.max_return_primes,
            include_per_node=args.include_per_node,
        )
        
        resp = stub.Compute(req, timeout=3600)
        
        # Convert protobuf response to dict format matching HTTP response
        out = {
            "ok": True,
            "total_primes": resp.total_primes,
            "max_prime": resp.max_prime,
            "elapsed_seconds": resp.elapsed_seconds,
            "primes_truncated": resp.primes_truncated,
            "secondary_exec": args.secondary_exec,
            "nodes_used": resp.nodes_used,
        }
        if return_list:
            out["primes"] = list(resp.primes)
        
        # Add per_node if present
        if resp.per_node:
            per_node_list = []
            for pn in resp.per_node:
                per_node_list.append({
                    "node_id": pn.node_id,
                    "slice": list(pn.slice),
                    "total_primes": pn.total_primes,
                    "node_elapsed_s": pn.node_elapsed_s,
                    "round_trip_s": pn.round_trip_s,
                })
            out["per_node"] = per_node_list
        
        return out
    except Exception as e:
        return {"ok": False, "error": f"gRPC failed: {e}; will try HTTP"}


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
        
        # Try gRPC first, fallback to HTTP
        resp = _grpc_compute(args.primary, args, return_list)
        if not resp.get("ok"):
            # Fallback to HTTP
            payload = {
                "low": args.low,
                "high": args.high,
                "mode": "list" if return_list else "count",
                "chunk": args.chunk,
                "secondary_exec": args.secondary_exec,
                "secondary_workers": args.secondary_workers,
                "max_return_primes": args.max_return_primes,
                "include_per_node": args.include_per_node,
            }
            url = args.primary.rstrip("/") + "/compute"
            resp = _post_json(url, payload, timeout_s=3600)
        
        t1 = time.perf_counter()

################################################################################
#NOTE this is almost entirely unchecked (generated), but it's "just" reporting #
        if not resp.get("ok"):
            print(f"Distributed error: {resp}", file=sys.stderr)
            return 1

        if resp.get("partial_failure"):
            print("\n[!] WARNING: Partial result received. Some nodes failed.", file=sys.stderr)
            if "failed_slices" in resp:
                for fail in resp["failed_slices"]:
                    print(f"    - Node {fail['node_id']} failed range {fail['slice']}: {fail['error']}", file=sys.stderr)
            print("-" * 40, file=sys.stderr)

        if args.mode == "count":
            suffix = " (PARTIAL)" if resp.get("partial_failure") else ""
            print(f"{int(resp.get('total_primes', 0))}{suffix}")
        else:
            primes = list(resp.get("primes", []))
            total = int(resp.get("total_primes", len(primes)))
            shown = primes[: args.max_print]
            print(f"Total primes: {total}")
            print(f"First {len(shown)} primes (from returned sample):")
            print(" ".join(map(str, shown)))
            if resp.get("primes_truncated") or total > len(primes):
                print(f"... (returned primes are capped at {resp.get('max_return_primes', args.max_return_primes)})")
#NOTE: ie, improves correctness rather than robustness, per se... TODO         #
################################################################################

        if args.time:
            print(
                f"Elapsed seconds: {t1 - t0:.6f}  "
                f"(exec=distributed, nodes_used={resp.get('nodes_used')}, secondary_exec={resp.get('secondary_exec')}, chunk={args.chunk})",
                file=sys.stderr,
            )
            if args.include_per_node and "per_node" in resp:               #TODO
                print("Per-node summary:", file=sys.stderr)
                for r in resp["per_node"]:
                    print(
                        f"  {r['node_id']:>12} slice={r['slice']} primes={r['total_primes']} "
                        f"node_elapsed={r['node_elapsed_s']:.3f}s round_trip={r['round_trip_s']:.3f}s",
                        file=sys.stderr,
                    )
        return 0

    # Local paths
    ranges = iter_ranges(args.low, args.high, args.chunk)
    t0 = time.perf_counter()
    results: List[Tuple[int, int, object]] = []

    if args.exec == "single":
        for a, b in ranges:
            results.append(_work_chunk((a, b, return_list)))

    elif args.exec == "threads":
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, return_list)) for a, b in ranges]
            for f in as_completed(futs):
                results.append(f.result())

    else:  # processes
        with ProcessPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, return_list)) for a, b in ranges]
            for f in as_completed(futs):
                results.append(f.result())

    t1 = time.perf_counter()
    results.sort(key=lambda x: x[0])

    if args.mode == "count":
        total = 0
        for _, _, res in results:
            total += int(res)  # type: ignore[arg-type]
        print(total)
    else:
        all_primes: List[int] = []
        for _, _, res in results:
            all_primes.extend(list(res))  # type: ignore[arg-type]
        total = len(all_primes)
        shown = all_primes[: args.max_print]
        print(f"Total primes: {total}")
        print(f"First {len(shown)} primes:")
        print(" ".join(map(str, shown)))
        if total > len(shown):
            print(f"... ({total - len(shown)} more not shown)")

    if args.time:
        print(
            f"Elapsed seconds: {t1 - t0:.6f}  "
            f"(exec={args.exec}, workers={args.workers if args.exec!='single' else 1}, chunks={len(ranges)}, chunk_size={args.chunk})",
            file=sys.stderr,
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
