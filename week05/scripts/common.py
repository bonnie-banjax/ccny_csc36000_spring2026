from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
RUNTIME_DIR = ROOT / ".runtime"
CLUSTER_JSON = RUNTIME_DIR / "cluster.json"


def default_cluster(host: str, replica_start_port: int) -> dict[str, Any]:
    reps: list[dict[str, Any]] = []
    for rid in range(1, 6):
        reps.append(
            {
                "id": rid,
                "addr": f"{host}:{replica_start_port + (rid - 1)}",
                "pid": None,
            }
        )
    return {
        "gateway": {"addr": f"{host}:50051", "pid": None},
        "replicas": reps,
    }


def load_or_init_cluster(host: str, replica_start_port: int) -> dict[str, Any]:
    if CLUSTER_JSON.exists():
        try:
            return json.loads(CLUSTER_JSON.read_text())
        except Exception:
            pass
    return default_cluster(host, replica_start_port)


def load_cluster_if_present() -> dict[str, Any] | None:
    if not CLUSTER_JSON.exists():
        return None
    try:
        return json.loads(CLUSTER_JSON.read_text())
    except Exception:
        return None


def find_or_create_replica_entry(
    data: dict[str, Any], replica_id: int, host: str, replica_start_port: int
) -> dict[str, Any]:
    for rep in data.get("replicas", []):
        if int(rep.get("id", -1)) == replica_id:
            return rep

    rep = {
        "id": replica_id,
        "addr": f"{host}:{replica_start_port + (replica_id - 1)}",
        "pid": None,
    }
    data.setdefault("replicas", []).append(rep)
    data["replicas"] = sorted(data["replicas"], key=lambda r: int(r.get("id", 0)))
    return rep


def best_effort_stop_pid(pid: Any) -> None:
    if not isinstance(pid, int):
        return
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass
    else:
        deadline = time.time() + 3.0
        while time.time() < deadline:
            try:
                os.kill(pid, 0)
            except OSError:
                return
            time.sleep(0.05)
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


def collect_cluster_pids(data: dict[str, Any]) -> list[int]:
    pids: list[int] = []
    gw_pid = data.get("gateway", {}).get("pid")
    if isinstance(gw_pid, int):
        pids.append(gw_pid)

    for rep in data.get("replicas", []):
        pid = rep.get("pid")
        if isinstance(pid, int):
            pids.append(pid)

    return pids


def null_out_all_pids(data: dict[str, Any]) -> None:
    if "gateway" in data and isinstance(data["gateway"], dict):
        data["gateway"]["pid"] = None
    for rep in data.get("replicas", []):
        if isinstance(rep, dict):
            rep["pid"] = None


def best_effort_stop_cluster_pids(data: dict[str, Any]) -> None:
    for pid in collect_cluster_pids(data):
        best_effort_stop_pid(pid)


def best_effort_stop_listening_ports(port_numbers: list[int]) -> None:
    discovered_listener_pids: set[int] = set()
    for port_number in port_numbers:
        proc = subprocess.run(
            ["lsof", "-t", f"-iTCP:{port_number}", "-sTCP:LISTEN"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if proc.returncode not in (0, 1):
            continue
        for line in proc.stdout.splitlines():
            try:
                discovered_listener_pids.add(int(line.strip()))
            except ValueError:
                continue

    for listener_pid in sorted(discovered_listener_pids):
        best_effort_stop_pid(listener_pid)


def extract_cluster_ports(data: dict[str, Any]) -> list[int]:
    port_numbers: list[int] = []
    gateway_address = str(data.get("gateway", {}).get("addr", ""))
    if ":" in gateway_address:
        try:
            port_numbers.append(int(gateway_address.rsplit(":", 1)[1]))
        except ValueError:
            pass

    for replica_entry in data.get("replicas", []):
        replica_address = str(replica_entry.get("addr", ""))
        if ":" in replica_address:
            try:
                port_numbers.append(int(replica_address.rsplit(":", 1)[1]))
            except ValueError:
                pass

    return sorted(set(port_numbers))


def start_process(cmd: list[str], log_path: Path) -> subprocess.Popen[Any]:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_fd = os.open(log_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    stdin_fd = os.open(os.devnull, os.O_RDONLY)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdin=stdin_fd,
            stdout=log_fd,
            stderr=subprocess.STDOUT,
            start_new_session=True,
            close_fds=True,
        )
    finally:
        os.close(log_fd)
        os.close(stdin_fd)
    return proc


def wait_for_address(addr: str, timeout_seconds: float) -> bool:
    if ":" not in addr:
        return False

    host, raw_port = addr.rsplit(":", 1)
    try:
        port = int(raw_port)
    except ValueError:
        return False

    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.25):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def start_process_and_wait_until_ready(
    cmd: list[str],
    log_path: Path,
    addr: str,
    startup_timeout_seconds: float = 10.0,
) -> subprocess.Popen[Any]:
    proc = start_process(cmd, log_path)
    deadline = time.time() + startup_timeout_seconds

    while time.time() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"process exited before becoming ready for {addr}")
        if wait_for_address(addr, timeout_seconds=0.2):
            return proc
        time.sleep(0.05)

    best_effort_stop_pid(proc.pid)
    raise RuntimeError(f"timed out waiting for {addr} to become ready")


def write_cluster(data: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    CLUSTER_JSON.write_text(json.dumps(data, indent=2))
