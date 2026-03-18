#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from common import (
    ROOT,
    RUNTIME_DIR,
    best_effort_stop_pid,
    best_effort_stop_listening_ports,
    find_or_create_replica_entry,
    load_or_init_cluster,
    start_process_and_wait_until_ready,
    write_cluster,
)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Start one replica and update its PID in .runtime/cluster.json"
    )
    p.add_argument("replica_id", type=int)
    p.add_argument("--host", default="127.0.0.1", help="Replica host")
    p.add_argument("--replica-start-port", type=int, default=50061)
    return p.parse_args()

def main() -> int:
    args = _parse_args()
    if args.replica_id < 1:
        print("replica_id must be >= 1", file=sys.stderr)
        return 2
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

    data = load_or_init_cluster(args.host, args.replica_start_port)
    rep = find_or_create_replica_entry(
        data, args.replica_id, args.host, args.replica_start_port
    )

    existing_pid = rep.get("pid")
    if isinstance(existing_pid, int):
        best_effort_stop_pid(existing_pid)

    port = args.replica_start_port + (args.replica_id - 1)
    best_effort_stop_listening_ports([port])
    host = args.host
    addr = f"{host}:{port}"

    proc = start_process_and_wait_until_ready(
        [
            sys.executable,
            str(ROOT / "replica_admin.py"),
            "--host",
            host,
            "--port",
            str(port),
        ],
        RUNTIME_DIR / f"replica_{args.replica_id}.log",
        addr=addr,
    )

    rep["addr"] = addr
    rep["pid"] = proc.pid

    write_cluster(data)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
