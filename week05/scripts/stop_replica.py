#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import sys

from common import (
    best_effort_stop_pid,
    find_or_create_replica_entry,
    load_or_init_cluster,
    write_cluster,
)

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from raft_support import ReplicaAvailabilityMessage, ReplicaTransportClient


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stop one replica and set its PID to null in .runtime/cluster.json"
    )
    p.add_argument("replica_id", type=int)
    p.add_argument("--host", default="127.0.0.1", help="Replica host")
    p.add_argument("--replica-start-port", type=int, default=50061)
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    if args.replica_id < 1:
        return 2

    data = load_or_init_cluster(args.host, args.replica_start_port)
    rep = find_or_create_replica_entry(
        data, args.replica_id, args.host, args.replica_start_port
    )

    pid = rep.get("pid")
    if isinstance(pid, int):
        control_client = None
        try:
            control_client = ReplicaTransportClient(str(rep["addr"]))
            control_client.set_replica_availability(
                ReplicaAvailabilityMessage(paused=True),
                timeout_seconds=2.0,
            )
        except Exception:
            best_effort_stop_pid(pid)
            rep["pid"] = None
        finally:
            if control_client is not None:
                control_client.close()

    write_cluster(data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
