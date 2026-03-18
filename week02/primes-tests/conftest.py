from __future__ import annotations

from pathlib import Path

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--impl-dir",
        action="store",
        default=None,
        help="Path to the week02 implementation directory (contains primary_node.py, secondary_node.py, primes_pb2.py, primes_pb2_grpc.py).",
    )
    parser.addoption(
        "--pass-ttl",
        action="store_true",
        default=False,
        help="If set, pass --ttl to primary/secondary node startup commands. Default is false.",
    )


@pytest.fixture(scope="session")
def impl_dir(pytestconfig: pytest.Config) -> Path:
    raw = pytestconfig.getoption("impl_dir")
    if raw is None:
        raise pytest.UsageError("Missing --impl-dir. Example: pytest kbrown6/week02/primes-tests --impl-dir group7/week02")

    path = Path(raw).expanduser().resolve()
    required = ("primary_node.py", "secondary_node.py", "primes_pb2.py", "primes_pb2_grpc.py")
    missing = [name for name in required if not (path / name).exists()]
    if missing:
        raise pytest.UsageError(f"--impl-dir does not look valid: {path} (missing: {', '.join(missing)})")
    return path


@pytest.fixture(scope="session")
def pass_ttl(pytestconfig: pytest.Config) -> bool:
    return bool(pytestconfig.getoption("pass_ttl"))
