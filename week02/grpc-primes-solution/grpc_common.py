from __future__ import annotations

import os
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Tuple

from primes_in_range import get_primes


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


def split_into_slices(low: int, high: int, n: int) -> List[Tuple[int, int]]:
    """Split [low, high) into n nearly equal contiguous slices."""
    if n <= 0:
        return []
    total = high - low
    base = total // n
    rem = total % n
    out: List[Tuple[int, int]] = []
    start = low
    for i in range(n):
        size = base + (1 if i < rem else 0)
        end = start + size
        if start < end:
            out.append((start, end))
        start = end
    return out


def _work_chunk(args: Tuple[int, int, bool]) -> Dict[str, Any]:
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
        "max_prime": -1,
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
    Compute primes in [low, high) by splitting into chunks and executing
    each chunk in single-threaded, threaded, or process mode.
    """
    if high <= low:
        raise ValueError("high must be > low")
    if mode not in ("count", "list"):
        raise ValueError("mode must be 'count' or 'list'")
    if exec_mode not in ("single", "threads", "processes"):
        raise ValueError("exec must be single|threads|processes")

    if workers is None:
        workers = os.cpu_count() or 4
    workers = max(1, int(workers))

    ranges = iter_ranges(low, high, chunk)
    want_list = (mode == "list")

    t0 = time.perf_counter()
    chunk_results: List[Dict[str, Any]] = []

    if exec_mode == "single":
        for a, b in ranges:
            chunk_results.append(_work_chunk((a, b, want_list)))
    elif exec_mode == "threads":
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())
    else:
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())

    t1 = time.perf_counter()

    chunk_results.sort(key=lambda d: int(d["low"]))
    total_primes = sum(int(d["prime_count"]) for d in chunk_results)
    sum_chunk = sum(float(d["elapsed_s"]) for d in chunk_results)

    primes_out: List[int] | None = None
    truncated = False
    max_prime = -1

    if want_list:
        primes_out = []
        for d in chunk_results:
            ps = d.get("primes") or []
            if ps:
                max_prime = max(max_prime, int(ps[-1]))
            if len(primes_out) < max_return_primes:
                remaining = max_return_primes - len(primes_out)
                primes_out.extend(ps[:remaining])
                if len(ps) > remaining:
                    truncated = True
            else:
                truncated = True

    response: Dict[str, Any] = {
        "ok": True,
        "mode": mode,
        "range": [low, high],
        "chunk": chunk,
        "exec": exec_mode,
        "workers": workers if exec_mode != "single" else 1,
        "chunks": len(ranges),
        "total_primes": total_primes,
        "max_prime": max_prime,
        "elapsed_seconds": t1 - t0,
        "sum_chunk_compute_seconds": sum_chunk,
    }

    if include_per_chunk:
        slim = []
        for d in chunk_results:
            slim.append(
                {
                    "low": d["low"],
                    "high": d["high"],
                    "elapsed_s": d["elapsed_s"],
                    "prime_count": d["prime_count"],
                    "max_prime": d.get("max_prime", -1),
                }
            )
        response["per_chunk"] = slim

    if primes_out is not None:
        response["primes"] = primes_out
        response["primes_truncated"] = truncated
        response["max_return_primes"] = max_return_primes

    return response
