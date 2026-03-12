from __future__ import annotations

import os
import socket
import time

import grpc
import pytest

import primes_pb2  # type: ignore
import primes_pb2_grpc  # type: ignore

from primary_node import serve as serve_primary
from secondary_node import register_once, serve as serve_worker


def _reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def test_no_active_workers_error() -> None:
    primary_port = _reserve_port()
    primary_server, primary_port = serve_primary("127.0.0.1", primary_port, ttl_s=120)
    try:
        with grpc.insecure_channel(f"127.0.0.1:{primary_port}") as ch:
            stub = primes_pb2_grpc.CoordinatorServiceStub(ch)
            req = primes_pb2.ComputeRequest(
                low=2,
                high=100,
                mode=primes_pb2.COUNT,
                chunk=10,
                secondary_exec=primes_pb2.SINGLE,
                secondary_workers=1,
                include_per_node=False,
            )
            with pytest.raises(grpc.RpcError) as excinfo:
                stub.Compute(req, timeout=5.0)

        assert excinfo.value.code() == grpc.StatusCode.FAILED_PRECONDITION
        assert "no active secondary nodes" in (excinfo.value.details() or "")
    finally:
        primary_server.stop(0)


def test_bad_input_high_not_greater_than_low() -> None:
    primary_port = _reserve_port()
    worker_port = _reserve_port()
    primary_server, primary_port = serve_primary("127.0.0.1", primary_port, ttl_s=120)
    worker_server, worker_port = serve_worker("127.0.0.1", worker_port)
    try:
        rr = register_once(
            coordinator_host="127.0.0.1",
            coordinator_port=primary_port,
            node_id="worker-bad-input",
            host="127.0.0.1",
            port=worker_port,
            cpu_count=os.cpu_count() or 1,
        )
        assert rr.ok is True

        with grpc.insecure_channel(f"127.0.0.1:{primary_port}") as ch:
            stub = primes_pb2_grpc.CoordinatorServiceStub(ch)
            req = primes_pb2.ComputeRequest(
                low=100,
                high=100,
                mode=primes_pb2.COUNT,
                chunk=10,
                secondary_exec=primes_pb2.SINGLE,
                secondary_workers=1,
                include_per_node=False,
            )
            with pytest.raises(grpc.RpcError) as excinfo:
                stub.Compute(req, timeout=5.0)

        assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT
        assert "high must be > low" in (excinfo.value.details() or "")
    finally:
        worker_server.stop(0)
        primary_server.stop(0)


def test_integration_count_with_two_workers() -> None:
    primary_port = _reserve_port()
    worker1_port = _reserve_port()
    worker2_port = _reserve_port()
    primary_server, primary_port = serve_primary("127.0.0.1", primary_port, ttl_s=120)
    worker1_server, worker1_port = serve_worker("127.0.0.1", worker1_port)
    worker2_server, worker2_port = serve_worker("127.0.0.1", worker2_port)

    try:
        rr1 = register_once(
            coordinator_host="127.0.0.1",
            coordinator_port=primary_port,
            node_id="worker-it-1",
            host="127.0.0.1",
            port=worker1_port,
            cpu_count=os.cpu_count() or 1,
        )
        rr2 = register_once(
            coordinator_host="127.0.0.1",
            coordinator_port=primary_port,
            node_id="worker-it-2",
            host="127.0.0.1",
            port=worker2_port,
            cpu_count=os.cpu_count() or 1,
        )
        assert rr1.ok is True
        assert rr2.ok is True

        # Small delay to ensure registry view is fresh for Compute/ListNodes.
        time.sleep(0.05)

        with grpc.insecure_channel(f"127.0.0.1:{primary_port}") as ch:
            cstub = primes_pb2_grpc.CoordinatorServiceStub(ch)
            nodes = cstub.ListNodes(primes_pb2.Empty(), timeout=5.0)
            assert nodes.ok is True
            assert len(nodes.nodes) >= 2

            req = primes_pb2.ComputeRequest(
                low=2,
                high=1000,
                mode=primes_pb2.LIST,
                chunk=200,
                secondary_exec=primes_pb2.THREADS,
                secondary_workers=2,
                max_return_primes=15,
                include_per_node=True,
            )
            resp = cstub.Compute(req, timeout=20.0)

        assert resp.ok is True
        assert resp.nodes_used == 2
        assert len(resp.per_node) == 2
        assert list(resp.primes) == [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47]
        assert resp.primes_truncated is True
        assert resp.total_primes > len(resp.primes)
    finally:
        worker2_server.stop(0)
        worker1_server.stop(0)
        primary_server.stop(0)
