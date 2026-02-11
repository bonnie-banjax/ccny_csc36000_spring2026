#!/usr/bin/env python3
"""
secondary_node.py (gRPC)

Worker node that exposes prime-range computation via gRPC.

It can periodically register itself with primary_node.py using CoordinatorService.RegisterNode.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Tuple
from urllib.parse import urlparse

import grpc

# Make generated protobuf modules importable.
_GEN_DIR = Path(__file__).resolve().parent / "generated"
if str(_GEN_DIR) not in sys.path:
    sys.path.insert(0, str(_GEN_DIR))

import primes_pb2  # type: ignore
import primes_pb2_grpc  # type: ignore

from grpc_common import compute_partitioned


NODE_META = {}


def _mode_to_str(mode: int) -> str:
    if mode == primes_pb2.COUNT:
        return "count"
    if mode == primes_pb2.LIST:
        return "list"
    raise ValueError("mode must be COUNT or LIST")


def _exec_to_str(exec_mode: int) -> str:
    if exec_mode == primes_pb2.SINGLE:
        return "single"
    if exec_mode == primes_pb2.THREADS:
        return "threads"
    if exec_mode == primes_pb2.PROCESSES:
        return "processes"
    raise ValueError("exec must be SINGLE|THREADS|PROCESSES")


def _guess_local_ip_for(host: str, port: int) -> str:
    """Best-effort local interface IP used to reach host:port."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect((host, port))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"


def parse_primary_target(primary: str) -> Tuple[str, int]:
    """
    Accepts:
    - "http://127.0.0.1:50051"
    - "127.0.0.1:50051"
    """
    if "://" in primary:
        u = urlparse(primary)
        host = u.hostname or "127.0.0.1"
        port = int(u.port or 50051)
        return host, port
    if ":" in primary:
        h, p = primary.rsplit(":", 1)
        return h, int(p)
    return primary, 50051


class WorkerService(primes_pb2_grpc.WorkerServiceServicer):
    def Health(self, request: primes_pb2.Empty, context: grpc.ServicerContext) -> primes_pb2.HealthReply:
        return primes_pb2.HealthReply(ok=True, status="healthy")

    def ComputeRange(
        self, request: primes_pb2.ComputeRangeRequest, context: grpc.ServicerContext
    ) -> primes_pb2.ComputeRangeReply:
        low = int(request.low)
        high = int(request.high)
        chunk = int(request.chunk) if int(request.chunk) > 0 else 500_000
        workers = int(request.workers) if int(request.workers) > 0 else None
        max_return_primes = int(request.max_return_primes) if int(request.max_return_primes) > 0 else 5000
        include_per_chunk = bool(request.include_per_chunk)

        try:
            mode = _mode_to_str(int(request.mode))
            exec_mode_raw = int(request.exec)
            if exec_mode_raw == primes_pb2.EXEC_UNSPECIFIED:
                exec_mode_raw = primes_pb2.SINGLE
            exec_mode = _exec_to_str(exec_mode_raw)
        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return primes_pb2.ComputeRangeReply(ok=False, error=str(e))

        try:
            resp = compute_partitioned(
                low,
                high,
                mode=mode,
                chunk=chunk,
                exec_mode=exec_mode,
                workers=workers,
                max_return_primes=max_return_primes,
                include_per_chunk=include_per_chunk,
            )
        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return primes_pb2.ComputeRangeReply(ok=False, error=str(e))
        except Exception as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return primes_pb2.ComputeRangeReply(ok=False, error=str(e))

        per_chunk = []
        for d in resp.get("per_chunk", []):
            per_chunk.append(
                primes_pb2.ChunkSummary(
                    low=int(d["low"]),
                    high=int(d["high"]),
                    elapsed_s=float(d["elapsed_s"]),
                    prime_count=int(d["prime_count"]),
                    max_prime=int(d.get("max_prime", -1)),
                )
            )

        return primes_pb2.ComputeRangeReply(
            ok=True,
            total_primes=int(resp["total_primes"]),
            max_prime=int(resp.get("max_prime", -1)),
            elapsed_seconds=float(resp["elapsed_seconds"]),
            sum_chunk_compute_seconds=float(resp["sum_chunk_compute_seconds"]),
            primes=[int(x) for x in resp.get("primes", [])],
            primes_truncated=bool(resp.get("primes_truncated", False)),
            max_return_primes=int(resp.get("max_return_primes", max_return_primes)),
            per_chunk=per_chunk,
        )


def register_once(
    coordinator_host: str,
    coordinator_port: int,
    node_id: str,
    host: str,
    port: int,
    cpu_count: int,
) -> primes_pb2.RegisterNodeReply:
    req = primes_pb2.RegisterNodeRequest(
        node=primes_pb2.NodeInfo(
            node_id=node_id,
            host=host,
            port=port,
            cpu_count=cpu_count,
            last_seen_unix=int(time.time()),
        )
    )
    with grpc.insecure_channel(f"{coordinator_host}:{coordinator_port}") as channel:
        stub = primes_pb2_grpc.CoordinatorServiceStub(channel)
        return stub.RegisterNode(req, timeout=5.0)


def start_registration_loop(
    coordinator_host: str,
    coordinator_port: int,
    node_id: str,
    host: str,
    port: int,
    interval_s: int = 3600,
) -> None:
    def loop() -> None:
        while True:
            try:
                register_once(
                    coordinator_host=coordinator_host,
                    coordinator_port=coordinator_port,
                    node_id=node_id,
                    host=host,
                    port=port,
                    cpu_count=(os.cpu_count() or 1),
                )
            except Exception as e:
                # keep looping through transient network failures
                print(f"[secondary_node] registration failed: {e}", flush=True)
            time.sleep(interval_s)

    th = threading.Thread(target=loop, daemon=True)
    th.start()


def serve(host: str, port: int) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(8, os.cpu_count() or 4)))
    primes_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    if bound_port == 0:
        raise RuntimeError(f"failed to bind worker on {host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    ap = argparse.ArgumentParser(description="Secondary prime worker node (gRPC server).")
    ap.add_argument("--host", default="127.0.0.1", help="Bind host.")
    ap.add_argument("--port", type=int, default=50061, help="Bind port.")
    ap.add_argument("--node-id", default=None, help="Optional stable node id (default: hostname).")

    # Compatibility: keep --primary from old HTTP CLI, now interpreted as host:port/URL for gRPC coordinator.
    ap.add_argument("--primary", default=None, help="Coordinator target, e.g. 127.0.0.1:50051 or http://127.0.0.1:50051")
    ap.add_argument("--public-host", default=None, help="Host/IP to advertise to coordinator (default auto-detect).")
    ap.add_argument("--register-interval", type=int, default=3600, help="Seconds between heartbeats.")

    # explicit coordinator flags (optional, override --primary when provided)
    ap.add_argument("--coordinator-host", default=None)
    ap.add_argument("--coordinator-port", type=int, default=None)

    args = ap.parse_args()

    node_id = args.node_id or os.uname().nodename

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

    advertised_host = args.public_host
    if coord_host and coord_port and not advertised_host:
        advertised_host = _guess_local_ip_for(coord_host, coord_port)
    if not advertised_host:
        advertised_host = "127.0.0.1"

    server, bound_port = serve(args.host, args.port)

    NODE_META.update(
        {
            "node_id": node_id,
            "bind_host": args.host,
            "bind_port": bound_port,
            "advertised_host": advertised_host,
            "advertised_port": bound_port,
            "cpu_count": os.cpu_count() or 1,
            "registered_to": f"{coord_host}:{coord_port}" if coord_host and coord_port else None,
        }
    )

    if coord_host and coord_port:
        start_registration_loop(
            coordinator_host=coord_host,
            coordinator_port=coord_port,
            node_id=node_id,
            host=advertised_host,
            port=bound_port,
            interval_s=max(5, int(args.register_interval)),
        )

    print(f"[secondary_node] node_id={node_id}")
    print(f"[secondary_node] gRPC listening on {args.host}:{bound_port}")
    print(f"[secondary_node] advertised as {advertised_host}:{bound_port}")
    if coord_host and coord_port:
        print(f"[secondary_node] registering to coordinator: {coord_host}:{coord_port}")
    print("  RPC Health")
    print("  RPC ComputeRange")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\n[secondary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        server.stop(grace=3)
    finally:
        print("[secondary_node] server stopped.")


if __name__ == "__main__":
    main()
