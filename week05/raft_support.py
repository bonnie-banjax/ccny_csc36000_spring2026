from __future__ import annotations

import json
import pickle
import threading
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import grpc


PROJECT_ROOT = Path(__file__).resolve().parent
RUNTIME_DIRECTORY = PROJECT_ROOT / ".runtime"
CLUSTER_METADATA_PATH = RUNTIME_DIRECTORY / "cluster.json"

DEFAULT_GATEWAY_PORT = 50051
DEFAULT_REPLICA_START_PORT = 50061
DEFAULT_REPLICA_COUNT = 5

REQUEST_VOTE_METHOD = "/raft.Internal/RequestVote"
APPEND_ENTRIES_METHOD = "/raft.Internal/AppendEntries"
CLIENT_WRITE_METHOD = "/raft.Internal/ClientWrite"
READ_CONVERSATION_METHOD = "/raft.Internal/ReadConversation"
SET_REPLICA_AVAILABILITY_METHOD = "/raft.Internal/SetReplicaAvailability"


def current_time_millis() -> int:
    return int(time.time() * 1000)


def ordered_conversation_participants(first_user: str, second_user: str) -> tuple[str, str]:
    """Return the canonical tuple used by the assignment for a direct conversation."""
    return (first_user, second_user) if first_user <= second_user else (second_user, first_user)


def load_cluster_metadata() -> dict[str, Any] | None:
    if not CLUSTER_METADATA_PATH.exists():
        return None

    try:
        return json.loads(CLUSTER_METADATA_PATH.read_text())
    except Exception:
        return None


def infer_replica_id_from_port(listen_port: int, replica_start_port: int = DEFAULT_REPLICA_START_PORT) -> int:
    return (listen_port - replica_start_port) + 1


def infer_replica_addresses(
    host: str,
    replica_count: int = DEFAULT_REPLICA_COUNT,
    replica_start_port: int = DEFAULT_REPLICA_START_PORT,
) -> list[str]:
    """
    Prefer explicit addresses from .runtime/cluster.json when available.
    Fall back to the default host/port contract from the assignment.
    """
    cluster_metadata = load_cluster_metadata()
    if cluster_metadata:
        replica_entries = sorted(cluster_metadata.get("replicas", []), key=lambda replica: int(replica.get("id", 0)))
        addresses = [str(replica.get("addr", "")) for replica in replica_entries if replica.get("addr")]
        if addresses:
            return addresses

    return [
        f"{host}:{replica_start_port + replica_offset}"
        for replica_offset in range(replica_count)
    ]
@dataclass
class LogEntry:
    index: int
    term: int
    command_name: str
    payload: dict[str, Any]


@dataclass
class RequestVoteMessage:
    term: int
    candidate_id: int
    last_log_index: int
    last_log_term: int


@dataclass
class RequestVoteReply:
    term: int
    vote_granted: bool


@dataclass
class AppendEntriesMessage:
    term: int
    leader_id: int
    leader_address: str
    prev_log_index: int
    prev_log_term: int
    entries: list[dict[str, Any]]
    leader_commit: int


@dataclass
class AppendEntriesReply:
    term: int
    success: bool
    match_index: int


@dataclass
class ClientWriteMessage:
    from_user: str
    to_user: str
    client_id: str
    client_msg_id: str
    text: str


@dataclass
class ClientWriteReply:
    accepted: bool
    term: int
    leader_hint: str
    seq: int
    error_message: str


@dataclass
class ReadConversationMessage:
    user_a: str
    user_b: str
    after_seq: int
    limit: int


@dataclass
class StoredDirectEvent:
    seq: int
    from_user: str
    text: str
    server_time_ms: int
    client_id: str
    client_msg_id: str


@dataclass
class ReadConversationReply:
    accepted: bool
    term: int
    leader_hint: str
    served_by: list[str]
    events: list[dict[str, Any]]
    error_message: str


@dataclass
class ReplicaAvailabilityMessage:
    paused: bool


@dataclass
class ReplicaAvailabilityReply:
    acknowledged: bool


def serialize_dataclass(message_object: Any) -> bytes:
    return pickle.dumps(asdict(message_object), protocol=pickle.HIGHEST_PROTOCOL)


def deserialize_dataclass(raw_bytes: bytes, dataclass_type: type[Any]) -> Any:
    return dataclass_type(**pickle.loads(raw_bytes))


def create_dataclass_rpc_handler(request_type: type[Any], _response_type: type[Any], implementation):
    return grpc.unary_unary_rpc_method_handler(
        implementation,
        request_deserializer=lambda raw_bytes: deserialize_dataclass(raw_bytes, request_type),
        response_serializer=serialize_dataclass,
    )


class ReplicaTransportClient:
    """
    Small wrapper around grpc.Channel generic unary calls.

    The course protobuf files only expose the public testing surface.
    Internal Raft traffic therefore uses generic gRPC method names and pickle-serialized
    Python dataclasses to avoid introducing more .proto files.
    """

    def __init__(self, target_address: str):
        self.channel = grpc.insecure_channel(target_address)
        self._request_vote_call = self.channel.unary_unary(
            REQUEST_VOTE_METHOD,
            request_serializer=serialize_dataclass,
            response_deserializer=lambda raw_bytes: deserialize_dataclass(raw_bytes, RequestVoteReply),
        )
        self._append_entries_call = self.channel.unary_unary(
            APPEND_ENTRIES_METHOD,
            request_serializer=serialize_dataclass,
            response_deserializer=lambda raw_bytes: deserialize_dataclass(raw_bytes, AppendEntriesReply),
        )
        self._client_write_call = self.channel.unary_unary(
            CLIENT_WRITE_METHOD,
            request_serializer=serialize_dataclass,
            response_deserializer=lambda raw_bytes: deserialize_dataclass(raw_bytes, ClientWriteReply),
        )
        self._read_conversation_call = self.channel.unary_unary(
            READ_CONVERSATION_METHOD,
            request_serializer=serialize_dataclass,
            response_deserializer=lambda raw_bytes: deserialize_dataclass(raw_bytes, ReadConversationReply),
        )
        self._set_replica_availability_call = self.channel.unary_unary(
            SET_REPLICA_AVAILABILITY_METHOD,
            request_serializer=serialize_dataclass,
            response_deserializer=lambda raw_bytes: deserialize_dataclass(raw_bytes, ReplicaAvailabilityReply),
        )

    def request_vote(self, request_message: RequestVoteMessage, timeout_seconds: float) -> RequestVoteReply:
        return self._request_vote_call(request_message, timeout=timeout_seconds)

    def append_entries(self, request_message: AppendEntriesMessage, timeout_seconds: float) -> AppendEntriesReply:
        return self._append_entries_call(request_message, timeout=timeout_seconds)

    def client_write(self, request_message: ClientWriteMessage, timeout_seconds: float) -> ClientWriteReply:
        return self._client_write_call(request_message, timeout=timeout_seconds)

    def read_conversation(self, request_message: ReadConversationMessage, timeout_seconds: float) -> ReadConversationReply:
        return self._read_conversation_call(request_message, timeout=timeout_seconds)

    def set_replica_availability(self, request_message: ReplicaAvailabilityMessage, timeout_seconds: float) -> ReplicaAvailabilityReply:
        return self._set_replica_availability_call(request_message, timeout=timeout_seconds)

    def close(self) -> None:
        self.channel.close()


class ReplicaTransportPool:
    """Caches transport clients because the gateway and leader talk to replicas frequently."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._clients_by_address: dict[str, ReplicaTransportClient] = {}

    def get_client(self, target_address: str) -> ReplicaTransportClient:
        with self._lock:
            existing_client = self._clients_by_address.get(target_address)
            if existing_client is not None:
                return existing_client

            new_client = ReplicaTransportClient(target_address)
            self._clients_by_address[target_address] = new_client
            return new_client

    def close_all(self) -> None:
        with self._lock:
            clients = list(self._clients_by_address.values())
            self._clients_by_address.clear()

        for client in clients:
            client.close()
