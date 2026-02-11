import os
import sys
import time
from pathlib import Path

import grpc

# generated imports
GEN_DIR = Path(__file__).resolve().parents[1] / "generated"
if str(GEN_DIR) not in sys.path:
    sys.path.insert(0, str(GEN_DIR))

import primes_pb2  # type: ignore
import primes_pb2_grpc  # type: ignore

from primes_in_range import get_primes
from primary_node import serve as serve_primary
from secondary_node import serve as serve_worker, register_once


def test_distributed_count_end_to_end():
    primary_server, primary_port = serve_primary("127.0.0.1", 0, ttl_s=120)
    worker_server, worker_port = serve_worker("127.0.0.1", 0)

    try:
        # one-shot registration
        rr = register_once(
            coordinator_host="127.0.0.1",
            coordinator_port=primary_port,
            node_id="worker-it-1",
            host="127.0.0.1",
            port=worker_port,
            cpu_count=os.cpu_count() or 1,
        )
        assert rr.ok is True

        # give coordinator a tiny moment
        time.sleep(0.05)

        with grpc.insecure_channel(f"127.0.0.1:{primary_port}") as ch:
            stub = primes_pb2_grpc.CoordinatorServiceStub(ch)
            req = primes_pb2.ComputeRequest(
                low=2,
                high=50000,
                mode=primes_pb2.COUNT,
                chunk=5000,
                secondary_exec=primes_pb2.THREADS,
                secondary_workers=4,
                include_per_node=True,
            )
            resp = stub.Compute(req, timeout=30.0)

        assert resp.ok is True
        expected = int(get_primes(2, 50000, return_list=False))
        assert int(resp.total_primes) == expected
        assert resp.nodes_used >= 1
        assert len(resp.per_node) >= 1

    finally:
        worker_server.stop(0)
        primary_server.stop(0)
