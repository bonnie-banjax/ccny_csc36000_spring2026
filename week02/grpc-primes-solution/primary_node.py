#!/usr/bin/env python3
"""
primary_node.py (gRPC)

Coordinator service that:
1) Keeps an in-memory registry of worker nodes.
2) Fans out compute requests to active workers.
3) Aggregates worker results for count/list modes.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from concurrent import futures
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import grpc

# Make generated protobuf modules importable.
_GEN_DIR = Path(__file__).resolve().parent / "generated"
if str(_GEN_DIR) not in sys.path:
    sys.path.insert(0, str(_GEN_DIR))

import primes_pb2  # type: ignore
import primes_pb2_grpc  # type: ignore

from grpc_common import split_into_slices


@dataclass
class NodeRecord:
    node_id: str
    host: str
    port: int
    cpu_count: int
    last_seen_unix: int
    registered_at_unix: int


class Registry:
    def __init__(self, ttl_s: int = 3600):
        self.ttl_s = ttl_s
        self._lock = threading.Lock()
        self._nodes: Dict[str, NodeRecord] = {}

    def upsert(self, node: primes_pb2.NodeInfo) -> NodeRecord:
        now = int(time.time())
        rec = NodeRecord(
            node_id=str(node.node_id),
            host=str(node.host),
            port=int(node.port),
            cpu_count=int(node.cpu_count) if node.cpu_count else 1,
            last_seen_unix=int(node.last_seen_unix) if node.last_seen_unix else now,
            registered_at_unix=now,
        )
        with self._lock:
            old = self._nodes.get(rec.node_id)
            if old:
                rec.registered_at_unix = old.registered_at_unix
            self._nodes[rec.node_id] = rec
        return rec

    def active_nodes(self) -> List[NodeRecord]:
        now = int(time.time())
        with self._lock:
            stale = [
                nid for nid, rec in self._nodes.items()
                if (now - int(rec.last_seen_unix)) > self.ttl_s
            ]
            for nid in stale:
                del self._nodes[nid]
            return list(self._nodes.values())


def _nodeinfo(rec: NodeRecord) -> primes_pb2.NodeInfo:
    return primes_pb2.NodeInfo(
        node_id=rec.node_id,
        host=rec.host,
        port=rec.port,
        cpu_count=rec.cpu_count,
        last_seen_unix=rec.last_seen_unix,
    )


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
    raise ValueError("secondary_exec must be SINGLE|THREADS|PROCESSES")


class CoordinatorService(primes_pb2_grpc.CoordinatorServiceServicer):
    def __init__(self, ttl_s: int):
        self.registry = Registry(ttl_s=ttl_s)

    def Health(self, request: primes_pb2.Empty, context: grpc.ServicerContext) -> primes_pb2.HealthReply:
        return primes_pb2.HealthReply(ok=True, status="healthy")

    def RegisterNode(
        self, request: primes_pb2.RegisterNodeRequest, context: grpc.ServicerContext
    ) -> primes_pb2.RegisterNodeReply:
        node = request.node
        if not node.node_id or not node.host or int(node.port) <= 0:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("node_id, host, and positive port are required")
            return primes_pb2.RegisterNodeReply(ok=False, error="invalid node info")
        rec = self.registry.upsert(node)
        print(
            f"[primary_node] registered node node_id={rec.node_id} "
            f"host={rec.host} port={rec.port} last_seen={rec.last_seen_unix}",
            flush=True,
        )
        return primes_pb2.RegisterNodeReply(ok=True, node=_nodeinfo(rec))

    def ListNodes(self, request: primes_pb2.Empty, context: grpc.ServicerContext) -> primes_pb2.ListNodesReply:
        nodes = sorted(self.registry.active_nodes(), key=lambda n: n.node_id)
        return primes_pb2.ListNodesReply(
            ok=True,
            nodes=[_nodeinfo(n) for n in nodes],
            ttl_s=self.registry.ttl_s,
        )

    def Compute(self, request: primes_pb2.ComputeRequest, context: grpc.ServicerContext) -> primes_pb2.ComputeReply:
        low = int(request.low)
        high = int(request.high)
        if high <= low:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("high must be > low")
            return primes_pb2.ComputeReply(ok=False, error="high must be > low")

        try:
            mode_str = _mode_to_str(int(request.mode))
        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return primes_pb2.ComputeReply(ok=False, error=str(e))

        sec_exec_val = int(request.secondary_exec)
        if sec_exec_val == primes_pb2.EXEC_UNSPECIFIED:
            sec_exec_val = primes_pb2.PROCESSES
        try:
            sec_exec_str = _exec_to_str(sec_exec_val)
        except ValueError as e:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return primes_pb2.ComputeReply(ok=False, error=str(e))

        sec_workers = int(request.secondary_workers)
        sec_workers_opt = sec_workers if sec_workers > 0 else None
        chunk = int(request.chunk) if int(request.chunk) > 0 else 500_000
        max_return_primes = int(request.max_return_primes) if int(request.max_return_primes) > 0 else 5000
        include_per_node = bool(request.include_per_node)

        nodes = self.registry.active_nodes()
        if not nodes:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details("no active secondary nodes registered")
            return primes_pb2.ComputeReply(ok=False, error="no active secondary nodes registered")

        nodes_sorted = sorted(nodes, key=lambda n: n.node_id)
        slices = split_into_slices(low, high, len(nodes_sorted))
        nodes_sorted = nodes_sorted[: len(slices)]

        t0 = time.perf_counter()
        per_node_results: List[primes_pb2.NodeResult] = []
        collected_primes: List[int] = []
        primes_truncated = False
        total_primes = 0
        max_prime = -1

        def call_node(node: NodeRecord, sl: Tuple[int, int]) -> primes_pb2.NodeResult:
            addr = f"{node.host}:{node.port}"
            req = primes_pb2.ComputeRangeRequest(
                low=sl[0],
                high=sl[1],
                mode=request.mode,
                chunk=chunk,
                exec=sec_exec_val,
                workers=sec_workers_opt or 0,
                max_return_primes=(max_return_primes if mode_str == "list" else 0),
                include_per_chunk=False,
            )
            t_call0 = time.perf_counter()
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = primes_pb2_grpc.WorkerServiceStub(channel)
                    reply = stub.ComputeRange(req, timeout=3600.0)
            except grpc.RpcError as e:
                details = e.details() if hasattr(e, "details") else str(e)
                raise RuntimeError(f"node {node.node_id} RPC error: {details}") from e

            t_call1 = time.perf_counter()
            if not reply.ok:
                raise RuntimeError(f"node {node.node_id} error: {reply.error}")

            return primes_pb2.NodeResult(
                node_id=node.node_id,
                node=_nodeinfo(node),
                slice_low=sl[0],
                slice_high=sl[1],
                round_trip_s=t_call1 - t_call0,
                node_elapsed_s=reply.elapsed_seconds,
                node_sum_chunk_s=reply.sum_chunk_compute_seconds,
                total_primes=reply.total_primes,
                max_prime=reply.max_prime,
            ), list(reply.primes), bool(reply.primes_truncated)

        errors: List[str] = []
        with futures.ThreadPoolExecutor(max_workers=min(32, len(nodes_sorted))) as ex:
            futs = [ex.submit(call_node, n, sl) for n, sl in zip(nodes_sorted, slices)]
            for fut in futures.as_completed(futs):
                try:
                    node_result, node_primes, node_trunc = fut.result()
                except Exception as e:
                    errors.append(str(e))
                    continue
                per_node_results.append(node_result)
                total_primes += int(node_result.total_primes)
                max_prime = max(max_prime, int(node_result.max_prime))
                if mode_str == "list":
                    if len(collected_primes) < max_return_primes:
                        remaining = max_return_primes - len(collected_primes)
                        collected_primes.extend(node_primes[:remaining])
                        if len(node_primes) > remaining:
                            primes_truncated = True
                    else:
                        primes_truncated = True
                    if node_trunc:
                        primes_truncated = True

        if errors:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details("; ".join(errors[:5]))
            return primes_pb2.ComputeReply(ok=False, error="; ".join(errors[:5]))

        per_node_results.sort(key=lambda r: int(r.slice_low))
        t1 = time.perf_counter()

        return primes_pb2.ComputeReply(
            ok=True,
            mode=request.mode,
            low=low,
            high=high,
            nodes_used=len(nodes_sorted),
            secondary_exec=sec_exec_val,
            secondary_workers=sec_workers_opt or 0,
            chunk=chunk,
            total_primes=total_primes,
            max_prime=max_prime,
            elapsed_seconds=t1 - t0,
            sum_node_compute_seconds=sum(float(r.node_elapsed_s) for r in per_node_results),
            sum_node_round_trip_seconds=sum(float(r.round_trip_s) for r in per_node_results),
            primes=collected_primes if mode_str == "list" else [],
            primes_truncated=primes_truncated if mode_str == "list" else False,
            max_return_primes=max_return_primes if mode_str == "list" else 0,
            per_node=per_node_results if include_per_node else [],
        )


def serve(host: str, port: int, ttl_s: int) -> tuple[grpc.Server, int]:
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max(8, os.cpu_count() or 4)))
    primes_pb2_grpc.add_CoordinatorServiceServicer_to_server(CoordinatorService(ttl_s=ttl_s), server)
    bound_port = server.add_insecure_port(f"{host}:{port}")
    if bound_port == 0:
        raise RuntimeError(f"failed to bind coordinator on {host}:{port}")
    server.start()
    return server, bound_port


def main() -> None:
    ap = argparse.ArgumentParser(description="Primary coordinator for distributed prime computation (gRPC).")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=50051)
    ap.add_argument("--ttl", type=int, default=3600, help="Seconds to keep node registrations alive.")
    args = ap.parse_args()

    server, bound_port = serve(args.host, args.port, ttl_s=max(10, int(args.ttl)))
    print(f"[primary_node] gRPC listening on {args.host}:{bound_port}")
    print("  RPC Health")
    print("  RPC ListNodes")
    print("  RPC RegisterNode")
    print("  RPC Compute")
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("\n[primary_node] KeyboardInterrupt received; shutting down gracefully...", flush=True)
        server.stop(grace=3)
    finally:
        print("[primary_node] server stopped.")


if __name__ == "__main__":
    main()
