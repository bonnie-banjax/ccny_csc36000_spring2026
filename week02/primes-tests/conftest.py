from __future__ import annotations

import sys
from pathlib import Path

import pytest

REQUIRED_FILES = (
    "primary_node.py",
    "secondary_node.py",
    "grpc_common.py",
    "primes_in_range.py",
)


def _has_required_files(path: Path) -> bool:
    return all((path / name).exists() for name in REQUIRED_FILES)


def _resolve_impl_dir(user_path: Path) -> Path:
    """
    Accept either:
    1) The exact implementation directory, or
    2) A parent directory containing exactly one matching implementation subtree.
    """
    if _has_required_files(user_path):
        return user_path

    matches = [p for p in user_path.rglob("*") if p.is_dir() and _has_required_files(p)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        found = ", ".join(str(m) for m in matches[:5])
        raise pytest.UsageError(
            "Ambiguous --impl-dir. Multiple implementation directories found under "
            f"{user_path}: {found}"
        )
    raise pytest.UsageError(
        "--impl-dir does not contain required files "
        f"({', '.join(REQUIRED_FILES)}): {user_path}"
    )


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--impl-dir",
        action="store",
        default=str(Path(__file__).resolve().parents[1] / "grpc-primes-solution"),
        help=(
            "Path to implementation directory containing primary_node.py, "
            "secondary_node.py, grpc_common.py, and primes_in_range.py"
        ),
    )


def pytest_configure(config: pytest.Config) -> None:
    impl_dir_input = Path(config.getoption("--impl-dir")).expanduser().resolve()
    if not impl_dir_input.exists() or not impl_dir_input.is_dir():
        raise pytest.UsageError(
            f"--impl-dir does not exist or is not a directory: {impl_dir_input}"
        )
    impl_dir = _resolve_impl_dir(impl_dir_input)
    config._resolved_impl_dir = impl_dir  # type: ignore[attr-defined]

    gen_dir = impl_dir / "generated"
    if str(impl_dir) not in sys.path:
        sys.path.insert(0, str(impl_dir))
    if gen_dir.exists() and str(gen_dir) not in sys.path:
        sys.path.insert(0, str(gen_dir))


@pytest.fixture(scope="session")
def impl_dir(pytestconfig: pytest.Config) -> Path:
    return getattr(pytestconfig, "_resolved_impl_dir")
