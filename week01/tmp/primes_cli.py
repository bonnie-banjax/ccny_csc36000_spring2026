#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from typing import Iterable, List, Tuple

from week01.primes_in_range import get_primes


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
    """
    Worker function for one chunk.
    Returns (low, high, result) where result is:
      - int (count) if return_list=False
      - List[int] / iterable if return_list=True
    """
    a, b, return_list = args
    res = get_primes(a, b, return_list=return_list)
    return (a, b, res)


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(
        description="Partitioned prime counting/listing over [low, high) using threads or processes."
    )
    ap.add_argument("--low", type=int, required=True, help="Range start (inclusive).")
    ap.add_argument("--high", type=int, required=True, help="Range end (exclusive). Must be > low.")
    ap.add_argument(
        "--mode",
        choices=["list", "count"],
        default="count",
        help="Output mode: list primes or just count them (default: count).",
    )
    ap.add_argument(
        "--chunk",
        type=int,
        default=500_000,
        help="Chunk size for partitioning the range (default: 500000).",
    )
    ap.add_argument(
        "--exec",
        choices=["single", "threads", "processes"],
        default="single",
        help="Execution strategy for chunks (default: single).",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=(os.cpu_count() or 4),
        help="Number of threads/processes for parallel exec (default: cpu_count).",
    )
    ap.add_argument(
        "--max-print",
        type=int,
        default=50,
        help="When mode=list, print at most this many primes (default: 50).",
    )
    ap.add_argument(
        "--time",
        action="store_true",
        help="Print timing information.",
    )

    args = ap.parse_args(argv)

    if args.high <= args.low:
        print("Error: --high must be > --low", file=sys.stderr)
        return 2

    return_list = (args.mode == "list")
    ranges = iter_ranges(args.low, args.high, args.chunk)

    t0 = time.perf_counter()

    # Collect per-chunk results as (low, high, result)
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

    # Sort by chunk start to keep deterministic ordering for list output.
    results.sort(key=lambda x: x[0])

    if args.mode == "count":
        # Each chunk returns an int count
        total = 0
        for _, _, res in results:
            total += int(res)  # type: ignore[arg-type]
        print(total)

    else:
        # Each chunk returns primes in that chunk; concatenate in order.
        # WARNING: for large ranges, this can be huge.
        all_primes: List[int] = []
        for _, _, res in results:
            # res could be list or iterable
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
