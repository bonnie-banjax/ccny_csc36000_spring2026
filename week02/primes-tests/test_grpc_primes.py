from __future__ import annotations

import importlib
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

grpc = pytest.importorskip("grpc")


def _find_open_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _set_first_existing_field(message: Any, candidates: list[str], value: Any) -> bool:
    for name in candidates:
        if hasattr(message, name):
            setattr(message, name, value)
            return True
    return False


def _get_first_existing_field(message: Any, candidates: list[str], default: Any = None) -> Any:
    for name in candidates:
        if hasattr(message, name):
            return getattr(message, name)
    return default


def _enum_value(pb2: Any, enum_name: str, member_name: str) -> int:
    if hasattr(pb2, member_name):
        return int(getattr(pb2, member_name))
    enum_obj = getattr(pb2, enum_name)
    return int(getattr(enum_obj, member_name))


def _first_message_instance(pb2: Any, candidates: list[str]) -> Any:
    for name in candidates:
        if hasattr(pb2, name):
            return getattr(pb2, name)()
    raise AssertionError(f"Could not find any message in {candidates}")


def _find_stub_class(pb2_grpc: Any) -> type:
    preferred = ["CoordinatorServiceStub", "CoordinatorStub"]
    for name in preferred:
        obj = getattr(pb2_grpc, name, None)
        if isinstance(obj, type):
            return obj

    for name, obj in vars(pb2_grpc).items():
        if name.endswith("Stub") and isinstance(obj, type) and "Worker" not in name:
            return obj
    raise AssertionError("Could not find coordinator stub class")


def _prime_list(low: int, high: int) -> list[int]:
    def is_prime(n: int) -> bool:
        if n < 2:
            return False
        if n == 2:
            return True
        if n % 2 == 0:
            return False
        d = 3
        while d * d <= n:
            if n % d == 0:
                return False
            d += 2
        return True

    return [n for n in range(low, high) if is_prime(n)]


def _launch_process(cmd: list[str], cwd: Path) -> subprocess.Popen[str]:
    return subprocess.Popen(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def _terminate_process(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


@dataclass
class Api:
    pb2: Any
    coordinator_stub_cls: type

    def empty_message(self) -> Any:
        return _first_message_instance(self.pb2, ["Empty", "ListNodesRequest", "HealthRequest"])

    def compute_request(
        self,
        *,
        low: int,
        high: int,
        mode: str,
        chunk: int,
        secondary_exec: str,
        secondary_workers: int,
        max_return_primes: int,
        include_per_node: bool,
    ) -> Any:
        req = _first_message_instance(self.pb2, ["ComputeRequest"])
        _set_first_existing_field(req, ["low"], low)
        _set_first_existing_field(req, ["high"], high)
        _set_first_existing_field(req, ["chunk"], chunk)
        _set_first_existing_field(req, ["max_return_primes"], max_return_primes)

        mode_val = _enum_value(self.pb2, "Mode", mode.upper())
        _set_first_existing_field(req, ["mode"], mode_val)

        exec_val = _enum_value(self.pb2, "ExecMode", secondary_exec.upper())
        _set_first_existing_field(req, ["sec_exec", "secondary_exec", "exec_mode", "exec"], exec_val)

        _set_first_existing_field(req, ["sec_workers", "secondary_workers", "workers"], secondary_workers)
        _set_first_existing_field(req, ["include_per_node", "include_per_chunk"], include_per_node)

        return req

    def register_node_request(self, *, node_id: str, host: str, port: int) -> Any:
        req = _first_message_instance(self.pb2, ["NodeMetadata", "RegisterNodeRequest"])
        _set_first_existing_field(req, ["node_id"], node_id)
        _set_first_existing_field(req, ["host"], host)
        _set_first_existing_field(req, ["port"], int(port))
        _set_first_existing_field(req, ["cpu_count"], 1)
        _set_first_existing_field(req, ["ts"], float(time.time()))
        return req


@pytest.fixture(scope="session")
def api(impl_dir: Path) -> Api:
    sys.path.insert(0, str(impl_dir))
    pb2 = importlib.import_module("primes_pb2")
    pb2_grpc = importlib.import_module("primes_pb2_grpc")
    coordinator_stub_cls = _find_stub_class(pb2_grpc)
    return Api(pb2=pb2, coordinator_stub_cls=coordinator_stub_cls)


@pytest.fixture()
def primary_server(impl_dir: Path, api: Api, pass_ttl: bool):
    port = _find_open_port()
    cmd = ["python3", "-u", "primary_node.py", "--host", "127.0.0.1", "--port", str(port)]
    if pass_ttl:
        cmd.extend(["--ttl", "120"])
    proc = _launch_process(cmd, impl_dir)
    channel = grpc.insecure_channel(f"127.0.0.1:{port}")
    stub = api.coordinator_stub_cls(channel)

    deadline = time.time() + 12
    last_error = "unknown"
    while time.time() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=1)
            raise AssertionError(f"primary exited early.\nstdout:\n{out}\nstderr:\n{err}")
        try:
            stub.ListNodes(api.empty_message(), timeout=1)
            break
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(0.2)
    else:
        _terminate_process(proc)
        raise AssertionError(f"primary did not become ready: {last_error}")

    try:
        yield {"port": port, "channel": channel, "stub": stub, "proc": proc}
    finally:
        channel.close()
        _terminate_process(proc)


def _start_secondary(impl_dir: Path, primary_port: int, node_id: str, pass_ttl: bool) -> tuple[subprocess.Popen[str], int]:
    variants = [
        f"127.0.0.1:{primary_port}",
        f"http://127.0.0.1:{primary_port}",
    ]
    last_details = ""
    for primary_value in variants:
        worker_port = _find_open_port()
        cmd = [
            "python3",
            "-u",
            "secondary_node.py",
            "--host",
            "127.0.0.1",
            "--port",
            str(worker_port),
            "--node-id",
            node_id,
            "--primary",
            primary_value,
            "--register-interval",
            "1",
        ]
        if pass_ttl:
            cmd.extend(["--ttl", "120"])
        proc = _launch_process(cmd, impl_dir)
        time.sleep(1.0)
        if proc.poll() is None:
            return proc, worker_port
        out, err = proc.communicate(timeout=1)
        last_details = f"variant primary={primary_value}\nstdout:\n{out}\nstderr:\n{err}"
    raise AssertionError(f"could not start secondary node.\n{last_details}")


@pytest.fixture()
def one_primary_two_workers(impl_dir: Path, primary_server: dict[str, Any], api: Api, pass_ttl: bool):
    workers: list[subprocess.Popen[str]] = []
    try:
        proc_a, port_a = _start_secondary(impl_dir, primary_server["port"], "worker-a", pass_ttl)
        proc_b, port_b = _start_secondary(impl_dir, primary_server["port"], "worker-b", pass_ttl)
        workers.extend([proc_a, proc_b])

        # Explicit registration keeps tests resilient if worker startup registration is buggy.
        primary_server["stub"].RegisterNode(
            api.register_node_request(node_id="worker-a", host="127.0.0.1", port=port_a),
            timeout=2,
        )
        primary_server["stub"].RegisterNode(
            api.register_node_request(node_id="worker-b", host="127.0.0.1", port=port_b),
            timeout=2,
        )

        # Some student implementations have a broken ListNodes RPC; avoid requiring it.
        time.sleep(0.8)
        yield primary_server
    finally:
        for proc in workers:
            _terminate_process(proc)


def _compute(stub: Any, request: Any) -> tuple[str, Any]:
    try:
        return ("response", stub.Compute(request, timeout=10))
    except grpc.RpcError as exc:
        return ("rpc_error", exc)


def _assert_failed_compute(result: tuple[str, Any]) -> None:
    kind, payload = result
    if kind == "rpc_error":
        assert payload.code() != grpc.StatusCode.OK
        return

    ok = _get_first_existing_field(payload, ["ok"], None)
    error_text = _get_first_existing_field(payload, ["error", "details", "message"], "")
    assert (ok is False) or bool(error_text), "expected an error response"


def _assert_successful_compute(result: tuple[str, Any]) -> Any:
    kind, payload = result
    if kind == "rpc_error":
        raise AssertionError(f"RPC failed unexpectedly: {payload.code()} {payload.details()}")
    ok = _get_first_existing_field(payload, ["ok"], True)
    assert ok is True
    return payload


@pytest.mark.correctness
def test_count_correctness_known_range(one_primary_two_workers: dict[str, Any], api: Api) -> None:
    request = api.compute_request(
        low=0,
        high=100,
        mode="count",
        chunk=20,
        secondary_exec="processes",
        secondary_workers=2,
        max_return_primes=1000,
        include_per_node=False,
    )
    response = _assert_successful_compute(_compute(one_primary_two_workers["stub"], request))
    expected_count = len(_prime_list(0, 100))
    actual = int(_get_first_existing_field(response, ["total_primes"], -1))
    assert actual == expected_count


@pytest.mark.correctness
def test_list_correctness_and_truncation(one_primary_two_workers: dict[str, Any], api: Api) -> None:
    request = api.compute_request(
        low=0,
        high=60,
        mode="list",
        chunk=10,
        secondary_exec="processes",
        secondary_workers=2,
        max_return_primes=5,
        include_per_node=False,
    )
    response = _assert_successful_compute(_compute(one_primary_two_workers["stub"], request))

    expected = _prime_list(0, 60)
    returned = list(_get_first_existing_field(response, ["primes"], []))
    total_primes = int(_get_first_existing_field(response, ["total_primes"], len(returned)))
    truncated = bool(_get_first_existing_field(response, ["primes_truncated"], False))

    assert total_primes == len(expected)
    assert returned == expected[:5]
    assert truncated is True


@pytest.mark.resilience
def test_no_active_workers_error(primary_server: dict[str, Any], api: Api) -> None:
    request = api.compute_request(
        low=0,
        high=100,
        mode="count",
        chunk=20,
        secondary_exec="processes",
        secondary_workers=2,
        max_return_primes=100,
        include_per_node=False,
    )
    _assert_failed_compute(_compute(primary_server["stub"], request))


@pytest.mark.resilience
def test_invalid_range_error_mapping(one_primary_two_workers: dict[str, Any], api: Api) -> None:
    request = api.compute_request(
        low=100,
        high=100,
        mode="count",
        chunk=20,
        secondary_exec="processes",
        secondary_workers=2,
        max_return_primes=100,
        include_per_node=False,
    )
    _assert_failed_compute(_compute(one_primary_two_workers["stub"], request))


@pytest.mark.grpc_impl
@pytest.mark.correctness
def test_integration_one_primary_two_workers(one_primary_two_workers: dict[str, Any], api: Api) -> None:
    request = api.compute_request(
        low=100,
        high=200,
        mode="count",
        chunk=25,
        secondary_exec="processes",
        secondary_workers=2,
        max_return_primes=100,
        include_per_node=True,
    )
    response = _assert_successful_compute(_compute(one_primary_two_workers["stub"], request))
    total = int(_get_first_existing_field(response, ["total_primes"], -1))
    nodes_used = int(_get_first_existing_field(response, ["nodes_used"], 0))
    assert total == len(_prime_list(100, 200))
    assert nodes_used >= 2


@pytest.mark.grpc_impl
def test_primary_exposes_coordinator_rpcs(primary_server: dict[str, Any], api: Api) -> None:
    response = primary_server["stub"].ListNodes(api.empty_message(), timeout=2)
    ok = _get_first_existing_field(response, ["ok"], True)
    assert ok in (True, False)


@pytest.mark.grpc_impl
def test_secondary_auto_registers_to_primary(
    impl_dir: Path,
    primary_server: dict[str, Any],
    api: Api,
    pass_ttl: bool,
) -> None:
    worker, _ = _start_secondary(impl_dir, primary_server["port"], "auto-register-check", pass_ttl=pass_ttl)
    try:
        deadline = time.time() + 8
        while time.time() < deadline:
            try:
                response = primary_server["stub"].ListNodes(api.empty_message(), timeout=1)
            except Exception:  # noqa: BLE001
                time.sleep(0.25)
                continue
            nodes = _get_first_existing_field(response, ["nodes"], [])
            if any(getattr(n, "node_id", "") == "auto-register-check" for n in nodes):
                return
            time.sleep(0.25)
        raise AssertionError("Secondary did not auto-register with primary using gRPC")
    finally:
        _terminate_process(worker)
