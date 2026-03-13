from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


RUBRIC_MAX = {
    "schema": 20.0,
    "correctness": 25.0,
    "grpc_impl": 20.0,
    "resilience": 15.0,
    "testing_repro": 10.0,
    "documentation": 10.0,
}

TEST_CATEGORY_POINTS = {
    "test_grpc_primes.py::test_count_correctness_known_range": [("correctness", 10.0)],
    "test_grpc_primes.py::test_list_correctness_and_truncation": [("correctness", 10.0)],
    "test_grpc_primes.py::test_integration_one_primary_two_workers": [("correctness", 5.0), ("grpc_impl", 5.0)],
    "test_grpc_primes.py::test_primary_exposes_coordinator_rpcs": [("grpc_impl", 5.0)],
    "test_grpc_primes.py::test_secondary_auto_registers_to_primary": [("grpc_impl", 10.0)],
    "test_grpc_primes.py::test_no_active_workers_error": [("resilience", 7.0)],
    "test_grpc_primes.py::test_invalid_range_error_mapping": [("resilience", 8.0)],
    "test_rubric_static.py::test_proto_services_and_enums": [("schema", 10.0)],
    "test_rubric_static.py::test_proto_avoids_stringly_mode_exec": [("schema", 5.0)],
    "test_rubric_static.py::test_generated_grpc_modules_exist": [("schema", 5.0)],
    "test_rubric_static.py::test_project_has_automated_test_artifacts": [("testing_repro", 10.0)],
    "test_rubric_static.py::test_project_has_readme": [("documentation", 5.0)],
    "test_rubric_static.py::test_readme_mentions_grpc_or_proto": [("documentation", 5.0)],
}


@dataclass
class GradeResult:
    project_dir: Path
    category_scores: dict[str, float]
    test_outcomes: dict[str, str]
    pytest_exit_code: int

    @property
    def total(self) -> float:
        return round(sum(self.category_scores.values()), 2)


def _canonical_test_id(classname: str, name: str) -> str:
    filename = classname.split(".")[-1] + ".py"
    return f"{filename}::{name}"


def _parse_junit(junit_path: Path) -> dict[str, str]:
    outcomes: dict[str, str] = {}
    root = ET.parse(junit_path).getroot()
    for tc in root.iter("testcase"):
        classname = tc.attrib.get("classname", "")
        name = tc.attrib.get("name", "")
        test_id = _canonical_test_id(classname, name)
        outcome = "passed"
        if tc.find("failure") is not None:
            outcome = "failed"
        elif tc.find("error") is not None:
            outcome = "error"
        elif tc.find("skipped") is not None:
            outcome = "skipped"
        outcomes[test_id] = outcome
    return outcomes


def _run_pytest_for_project(
    tests_dir: Path,
    project_dir: Path,
    python_bin: str,
    pass_ttl: bool,
) -> tuple[int, dict[str, str]]:
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tf:
        junit_path = Path(tf.name)

    cmd = [
        python_bin,
        "-m",
        "pytest",
        "-q",
        str(tests_dir),
        "--impl-dir",
        str(project_dir),
        f"--junitxml={junit_path}",
    ]
    if pass_ttl:
        cmd.append("--pass-ttl")
    env = os.environ.copy()
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)

    outcomes: dict[str, str] = {}
    if junit_path.exists():
        outcomes = _parse_junit(junit_path)
        junit_path.unlink(missing_ok=True)

    if proc.stdout:
        sys.stdout.write(proc.stdout)
    if proc.stderr:
        sys.stderr.write(proc.stderr)
    return proc.returncode, outcomes


def grade_project(tests_dir: Path, project_dir: Path, python_bin: str, pass_ttl: bool) -> GradeResult:
    category_scores = {k: 0.0 for k in RUBRIC_MAX}
    exit_code, outcomes = _run_pytest_for_project(tests_dir, project_dir, python_bin, pass_ttl)

    for test_id, category_items in TEST_CATEGORY_POINTS.items():
        if outcomes.get(test_id) == "passed":
            for category, points in category_items:
                category_scores[category] += points

    # Cap each category at rubric max.
    for category, cap in RUBRIC_MAX.items():
        category_scores[category] = min(cap, round(category_scores[category], 2))

    return GradeResult(
        project_dir=project_dir,
        category_scores=category_scores,
        test_outcomes=outcomes,
        pytest_exit_code=exit_code,
    )


def _print_grade(result: GradeResult) -> None:
    print(f"\nProject: {result.project_dir}")
    for key in ("schema", "correctness", "grpc_impl", "resilience", "testing_repro", "documentation"):
        print(f"  {key:14} {result.category_scores[key]:5.1f} / {RUBRIC_MAX[key]:.0f}")
    print(f"  {'TOTAL':14} {result.total:5.1f} / 100")
    if result.pytest_exit_code != 0:
        print(f"  note: pytest exit code {result.pytest_exit_code} (some checks failed or errored)")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run week02 prime tests and compute rubric grades for one or more project directories."
    )
    parser.add_argument(
        "project_dirs",
        nargs="+",
        help="One or more implementation directories (each should contain primary_node.py, secondary_node.py, primes_pb2.py, primes_pb2_grpc.py).",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Python executable to run pytest (default: current Python).",
    )
    parser.add_argument(
        "--pass-ttl",
        action="store_true",
        default=False,
        help="If set, pass --ttl to primary/secondary startup commands during grading.",
    )
    args = parser.parse_args()

    tests_dir = Path(__file__).resolve().parent
    any_failures = False
    for project in args.project_dirs:
        project_dir = Path(project).expanduser().resolve()
        result = grade_project(tests_dir, project_dir, args.python, args.pass_ttl)
        _print_grade(result)
        if result.pytest_exit_code != 0:
            any_failures = True
    return 1 if any_failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
