from __future__ import annotations

from pathlib import Path
from typing import List
import re

import pytest


@pytest.mark.schema
def test_proto_services_and_enums(impl_dir: Path) -> None:
    proto_path = impl_dir / "proto" / "primes.proto"
    assert proto_path.exists(), "Missing proto/primes.proto"
    text = proto_path.read_text(encoding="utf-8", errors="ignore")
    assert "service" in text
    assert ("CoordinatorService" in text) or re.search(r"\bservice\s+Coordinator\b", text)
    assert ("WorkerService" in text) or re.search(r"\bservice\s+Worker\b", text)
    assert re.search(r"\benum\s+Mode\b", text)
    assert re.search(r"\benum\s+ExecMode\b", text)


@pytest.mark.schema
def test_proto_avoids_stringly_mode_exec(impl_dir: Path) -> None:
    proto_path = impl_dir / "proto" / "primes.proto"
    assert proto_path.exists(), "Missing proto/primes.proto"
    text = proto_path.read_text(encoding="utf-8", errors="ignore")

    # Basic check: mode/exec-related fields should not be string-typed.
    forbidden = [
        r"\bstring\s+mode\b",
        r"\bstring\s+.*exec",
        r"\bstring\s+secondary_exec\b",
    ]
    for pattern in forbidden:
        assert not re.search(pattern, text), f"Found stringly-typed field matching: {pattern}"


@pytest.mark.schema
def test_generated_grpc_modules_exist(impl_dir: Path) -> None:
    assert (impl_dir / "primes_pb2.py").exists()
    assert (impl_dir / "primes_pb2_grpc.py").exists()


@pytest.mark.testing_repro
def test_project_has_automated_test_artifacts(impl_dir: Path) -> None:
    patterns = [
        "**/test_*.py",
        "**/*_test.py",
    ]
    has_tests = any(any(impl_dir.glob(pattern)) for pattern in patterns)
    has_pytest_cfg = (impl_dir / "pytest.ini").exists() or (impl_dir / "pyproject.toml").exists()
    assert has_tests or has_pytest_cfg, "Expected tests or pytest config in project"

def _get_files_ending_with_md_and_txt(impl_dir: Path) -> List[Path]:
    top_level_txt = [
        p for p in sorted(impl_dir.glob("*.txt")) if p.name.lower() != "requirements.txt"
    ]
    return sorted(impl_dir.glob("*.md")) + top_level_txt

@pytest.mark.documentation
def test_project_has_readme(impl_dir: Path) -> None:
    readme_candidates = [
        impl_dir / "README.md",
        impl_dir / "README.txt",
        impl_dir / "README",
    ] + _get_files_ending_with_md_and_txt(impl_dir)
    readme_path = next((p for p in readme_candidates if p.exists()), None)
    assert readme_path is not None, "Missing README in implementation directory"


@pytest.mark.documentation
def test_readme_mentions_grpc_or_proto(impl_dir: Path) -> None:
    readme_candidates = [
        impl_dir / "README.md",
        impl_dir / "README.txt",
        impl_dir / "README",
    ] + _get_files_ending_with_md_and_txt(impl_dir)
    readme_path = next((p for p in readme_candidates if p.exists()), None)
    assert readme_path is not None, "Missing README in implementation directory"
    text = readme_path.read_text(encoding="utf-8", errors="ignore").lower()
    assert ("grpc" in text) or ("protobuf" in text) or ("proto" in text)
