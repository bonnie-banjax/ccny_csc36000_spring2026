"""Compatibility wrapper for test harnesses expecting top-level protobuf modules."""

import grpc

from generated.primes_pb2_grpc import *  # noqa: F401,F403
from generated import primes_pb2 as _generated_pb2
from generated import primes_pb2_grpc as _generated_pb2_grpc


def _serialize_register_node_compat(request: object) -> bytes:
    if isinstance(request, _generated_pb2.RegisterNodeRequest):
        return request.SerializeToString()
    if isinstance(request, _generated_pb2.NodeInfo):
        return _generated_pb2.RegisterNodeRequest(node=request).SerializeToString()

    # Fallback for request-like objects with flat node fields.
    node = _generated_pb2.NodeInfo(
        node_id=str(getattr(request, "node_id", "")),
        host=str(getattr(request, "host", "")),
        port=int(getattr(request, "port", 0) or 0),
        cpu_count=int(getattr(request, "cpu_count", 1) or 1),
        last_seen_unix=int(getattr(request, "last_seen_unix", getattr(request, "ts", 0)) or 0),
    )
    return _generated_pb2.RegisterNodeRequest(node=node).SerializeToString()


class CoordinatorServiceStub(_generated_pb2_grpc.CoordinatorServiceStub):
    """Generated stub with a RegisterNode serializer compatible with flat node payloads."""

    def __init__(self, channel: grpc.Channel):
        super().__init__(channel)
        self.RegisterNode = channel.unary_unary(
            "/primes.CoordinatorService/RegisterNode",
            request_serializer=_serialize_register_node_compat,
            response_deserializer=_generated_pb2.RegisterNodeReply.FromString,
            _registered_method=True,
        )
