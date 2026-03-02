from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.develop import develop as _develop


ROOT = Path(__file__).resolve().parent
PROTO = ROOT / "direct.proto"


def compile_protos() -> None:
    """Generate *_pb2.py and *_pb2_grpc.py from direct.proto."""
    if not PROTO.exists():
        raise FileNotFoundError(f"Missing proto file: {PROTO}")

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        "-I",
        str(ROOT),
        "--python_out=.",
        "--grpc_python_out=.",
        str(PROTO),
    ]
    subprocess.check_call(cmd, cwd=str(ROOT))


class build_py(_build_py):
    def run(self):
        compile_protos()
        super().run()


class develop(_develop):
    def run(self):
        compile_protos()
        super().run()


setup(
    name="direct-chat",
    version="0.1.0",
    description="Direct (1:1) chat client + gateway protos",
    py_modules=[
        "direct_pb2",
        "direct_pb2_grpc",
        "direct_client",
    ],
    cmdclass={"build_py": build_py, "develop": develop},
    install_requires=[
        "grpcio>=1.60.0",
        "protobuf>=4.25.0",
    ],
)