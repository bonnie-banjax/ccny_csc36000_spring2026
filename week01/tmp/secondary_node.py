#!/usr/bin/env python3
"""
secondary_node.py

A "secondary node" HTTP server that exposes the same prime-range logic used by primes_cli.py,
but via HTTP requests.

- Partitions [low, high) into chunks (chunk_size)
- Computes each chunk using primes_in_range.get_primes(low, high, return_list=...)
- Supports execution modes:
    * single     : sequential chunks
    * threads    : ThreadPoolExecutor across chunks
    * processes  : ProcessPoolExecutor across chunks (best for CPU-bound work)

Endpoints
---------
GET  /health
    -> {"ok": true, "status": "healthy"}

POST /compute
    JSON body (all fields optional unless noted):
    {
      "low": 0,                   (required)
      "high": 1000000,            (required; exclusive)
      "mode": "count"|"list",     default "count"
      "chunk": 500000,            default 500000
      "exec": "single"|"threads"|"processes", default "single"
      "workers": 8,               default cpu_count
      "max_return_primes": 5000,  default 5000 (only used when mode="list")
      "include_per_chunk": true   default false (can be large)
    }

    Response (example):
    {
      "ok": true,
      "mode": "count",
      "range": [0, 1000000],
      "chunk": 200000,
      "exec": "processes",
      "workers": 8,
      "chunks": 5,
      "total_primes": 78498,
      "max_prime": 999983,
      "elapsed_seconds": 0.8421,
      "sum_chunk_compute_seconds": 5.4312,
      "per_chunk": [...]                 (optional)
      "primes": [2, 3, 5, ...]           (only if mode="list", capped)
      "primes_truncated": true|false     (only if mode="list")
    }

Why this is useful in a classroom:
- Run this server on several student machines.
- A "coordinator" can distribute subranges to each machine by calling /compute.
- Each node can further parallelize locally with threads/processes, showing hierarchical parallelism.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Tuple
from urllib.parse import urlparse


def _import_get_primes():
    """
    Try to import get_primes in a way that works whether your file layout is:
      - primes_in_range.py in the same folder, OR
      - week1/primes_in_range.py (as in the attached primes_cli.py)
    """
    try:
        from week1.primes_in_range import get_primes  # type: ignore
        return get_primes
    except Exception:
        from primes_in_range import get_primes  # type: ignore
        return get_primes


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
    Returns a dict so it serializes easily.
    """
    low, high, return_list = args
    get_primes = _import_get_primes()

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
    # In count mode we don't know max_prime without listing. Keep -1 to avoid extra work.
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
    Perform the partitioned computation using the primes_cli.py approach.
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

    else:  # processes
        # NOTE: For many chunks, passing data is fine. For large production use, consider a global pool.
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_work_chunk, (a, b, want_list)) for a, b in ranges]
            for f in as_completed(futs):
                chunk_results.append(f.result())

    t1 = time.perf_counter()

    # Sort by low for deterministic ordering
    chunk_results.sort(key=lambda d: int(d["low"]))

    total_primes = sum(int(d["prime_count"]) for d in chunk_results)
    sum_chunk = sum(float(d["elapsed_s"]) for d in chunk_results)

    # If mode=list, merge primes but cap to max_return_primes in response
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
        # if not truncated and primes_out exists, max_prime is already correct;
        # but if truncated early we still track max_prime using last of each chunk list.
        # max_prime is still correct because we used each chunk's last prime, not just what we returned.

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
        # Remove full primes arrays to keep response reasonable unless explicitly requested.
        slim = []
        for d in chunk_results:
            slim.append({
                "low": d["low"],
                "high": d["high"],
                "elapsed_s": d["elapsed_s"],
                "prime_count": d["prime_count"],
                "max_prime": d.get("max_prime", -1),
            })
        response["per_chunk"] = slim

    if primes_out is not None:
        response["primes"] = primes_out
        response["primes_truncated"] = truncated
        response["max_return_primes"] = max_return_primes

    return response


class Handler(BaseHTTPRequestHandler):
    server_version = "SecondaryPrimeNode/1.0"

    def _send_json(self, obj: Dict[str, Any], code: int = 200) -> None:
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            return self._send_json({"ok": True, "status": "healthy"})
        return self._send_json({"ok": False, "error": "not found"}, code=404)

    def do_POST(self):
        t0 = time.perf_counter()
        parsed = urlparse(self.path)
        if parsed.path != "/compute":
            return self._send_json({"ok": False, "error": "not found"}, code=404)

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except Exception:
            return self._send_json({"ok": False, "error": "invalid content-length"}, code=400)

        if length <= 0:
            return self._send_json({"ok": False, "error": "empty body"}, code=400)

        # Basic guardrail: keep requests from being enormous
        if length > 10 * 1024 * 1024:
            return self._send_json({"ok": False, "error": "request too large"}, code=413)

        body = self.rfile.read(length)
        try:
            payload = json.loads(body.decode("utf-8"))
        except Exception as e:
            return self._send_json({"ok": False, "error": f"bad json: {e}"}, code=400)

        try:
            low = int(payload["low"])
            high = int(payload["high"])
        except Exception:
            return self._send_json({"ok": False, "error": "payload must include integer low and high"}, code=400)

        mode = str(payload.get("mode", "count"))
        chunk = int(payload.get("chunk", 500_000))
        exec_mode = str(payload.get("exec", "single"))
        workers = payload.get("workers", None)
        if workers is not None:
            workers = int(workers)
        max_return_primes = int(payload.get("max_return_primes", 5000))
        include_per_chunk = bool(payload.get("include_per_chunk", False))

        try:
            resp = compute_partitioned(
                low, high,
                mode=mode,
                chunk=chunk,
                exec_mode=exec_mode,
                workers=workers,
                max_return_primes=max_return_primes,
                include_per_chunk=include_per_chunk,
            )
            json_code = (resp, 200)
        except Exception as e:
            json_code = ({"ok": False, "error": str(e)}, 400)
        finally:
            t1 = time.perf_counter()
            print(f"[secondary_node] Elapsed seconds: {t1 - t0:.6f} ")
            return self._send_json(json_code[0], code=json_code[1])

    def log_message(self, fmt, *args):
        # quieter logs for classroom use; comment out to debug
        return


def main() -> None:
    ap = argparse.ArgumentParser(description="Secondary prime worker node (HTTP server).")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9100)
    args = ap.parse_args()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"[secondary_node] listening on http://{args.host}:{args.port}")
    print("  GET  /health")
    print("  POST /compute  (JSON: low, high, mode, chunk, exec, workers, max_return_primes)")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[secondary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        httpd.shutdown()
    finally:
        httpd.server_close()
        print("[secondary_node] server stopped.")



if __name__ == "__main__":
    main()
