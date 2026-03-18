"""Compatibility wrapper for test harnesses expecting top-level protobuf modules."""

from generated.primes_pb2 import *  # noqa: F401,F403
from generated import primes_pb2 as _generated_pb2

# Some test harnesses look for NodeMetadata; this schema names it NodeInfo.
NodeMetadata = _generated_pb2.NodeInfo
