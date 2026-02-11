#!/usr/bin/env python3
"""
primes_cli.py

Supports:
- local execution (single|threads|processes)
- distributed execution via gRPC coordinator (exec=distributed)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urlparse

import grpc

# Make generated protobuf modules importable.
_GEN_DIR = Path(__file__).resolve().parent / "generated"
if str(_GEN_DIR) not in sys.path:
    sys.path.insert(0, str(_GEN_DIR))

import primes_pb2  # type: ignore
import primes_pb2_grpc  # type: ignore

from primes_in_range import get_primes


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


def parse_primary_target(primary: str) -> Tuple[str, int]:
    if "://" in primary:
        u = urlparse(primary)
        return u.hostname or "127.0.0.1", int(u.port or 50051)
    if ":" in primary:
        h, p = primary.rsplit(":", 1)
        return h, int(p)
    return primary, 50051


def _mode_to_enum(mode: str) -> int:
    return primes_pb2.LIST if mode == "list" else primes_pb2.COUNT


def _exec_to_enum(exec_mode: str) -> int:
    m = {
        "single": primes_pb2.SINGLE,
        "threads": primes_pb2.THREADS,
        "processes": primes_pb2.PROCESSES,
    }
    return m[exec_mode]


def _exec_enum_to_str(exec_mode: int) -> str:
    m = {
        primes_pb2.SINGLE: "single",
        primes_pb2.THREADS: "threads",
        primes_pb2.PROCESSES: "processes",
    }
    return m.get(exec_mode, str(exec_mode))


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Prime counting/listing over [low, high) using local threads/processes OR distributed worker nodes over gRPC."
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
    ap.add_argument("--primary", default=None, help="Coordinator target, e.g. 127.0.0.1:50051 or http://127.0.0.1:50051")
    ap.add_argument("--coordinator-host", default=None)
    ap.add_argument("--coordinator-port", type=int, default=None)

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
        coord_host = None
        coord_port = None

        if args.primary:
            coord_host, coord_port = parse_primary_target(args.primary)
        if args.coordinator_host:
            coord_host = args.coordinator_host
        if args.coordinator_port:
            coord_port = args.coordinator_port
        if coord_host is None and coord_port is not None:
            coord_host = "127.0.0.1"
        if coord_host is not None and coord_port is None:
            coord_port = 50051

        if coord_host is None or coord_port is None:
            print(
                "Error: provide --primary or both --coordinator-host/--coordinator-port when --exec distributed",
                file=sys.stderr,
            )
            return 2

        t0 = time.perf_counter()
        req = primes_pb2.ComputeRequest(
            low=args.low,
            high=args.high,
            mode=_mode_to_enum(args.mode),
            chunk=args.chunk,
            secondary_exec=_exec_to_enum(args.secondary_exec),
            secondary_workers=args.secondary_workers or 0,
            max_return_primes=args.max_return_primes,
            include_per_node=args.include_per_node,
        )

        try:
            with grpc.insecure_channel(f"{coord_host}:{coord_port}") as channel:
                stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
                resp = stub.Compute(req, timeout=3600.0)
        except grpc.RpcError as e:
            msg = e.details() if hasattr(e, "details") else str(e)
            print(f"Distributed RPC error: {msg}", file=sys.stderr)
            return 1

        t1 = time.perf_counter()

        if not resp.ok:
            print(f"Distributed error: {resp.error}", file=sys.stderr)
            return 1

        if args.mode == "count":
            print(int(resp.total_primes))
        else:
            primes = list(resp.primes)
            total = int(resp.total_primes) if resp.total_primes else len(primes)
            shown = primes[: args.max_print]
            print(f"Total primes: {total}")
            print(f"First {len(shown)} primes (from returned sample):")
            print(" ".join(map(str, shown)))
            if resp.primes_truncated or total > len(primes):
                cap = int(resp.max_return_primes) if resp.max_return_primes else args.max_return_primes
                print(f"... (returned primes are capped at {cap})")

        if args.time:
            print(
                f"Elapsed seconds: {t1 - t0:.6f}  "
                f"(exec=distributed, nodes_used={resp.nodes_used}, secondary_exec={_exec_enum_to_str(resp.secondary_exec)}, chunk={args.chunk})",
                file=sys.stderr,
            )
            if args.include_per_node and len(resp.per_node) > 0:
                print("Per-node summary:", file=sys.stderr)
                for r in sorted(resp.per_node, key=lambda x: x.slice_low):
                    print(
                        f"  {r.node_id:>12} slice={[int(r.slice_low), int(r.slice_high)]} primes={int(r.total_primes)} "
                        f"node_elapsed={float(r.node_elapsed_s):.3f}s round_trip={float(r.round_trip_s):.3f}s",
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
