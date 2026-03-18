#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import time
from concurrent import futures

import grpc

PROJECT_ROOT = __file__.rsplit("/", 1)[0]
GENERATED_DIRECTORY = f"{PROJECT_ROOT}/generated"
if GENERATED_DIRECTORY not in sys.path:
    sys.path.insert(0, GENERATED_DIRECTORY)

import direct_gateway_pb2
import direct_gateway_pb2_grpc
import replica_admin_pb2
import replica_admin_pb2_grpc

from raft_support import (
    ClientWriteMessage,
    DEFAULT_GATEWAY_PORT,
    DEFAULT_REPLICA_COUNT,
    DEFAULT_REPLICA_START_PORT,
    ReadConversationMessage,
    ReplicaTransportPool,
    infer_replica_addresses,
)


class GatewayReplicaDirectory:
    """Tracks replica addresses and offers helper methods for leader discovery."""

    def __init__(self, replica_addresses: list[str]):
        self.replica_addresses = list(replica_addresses)
        self.transport_pool = ReplicaTransportPool()
        self.status_channels_by_address: dict[str, grpc.Channel] = {}
        self.status_stubs_by_address: dict[str, replica_admin_pb2_grpc.ReplicaAdminStub] = {}
        self.cached_leader_address = ""
        self.last_reported_leader_address = ""

    def _log(self, message: str) -> None:
        print(f"[gateway directory] {message}", file=sys.stderr, flush=True)

    def close(self) -> None:
        self.transport_pool.close_all()
        for channel in self.status_channels_by_address.values():
            channel.close()
        self.status_channels_by_address.clear()
        self.status_stubs_by_address.clear()

    def get_transport_client(self, replica_address: str):
        return self.transport_pool.get_client(replica_address)

    def get_status_stub(self, replica_address: str) -> replica_admin_pb2_grpc.ReplicaAdminStub:
        existing_stub = self.status_stubs_by_address.get(replica_address)
        if existing_stub is not None:
            return existing_stub

        channel = grpc.insecure_channel(replica_address)
        stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
        self.status_channels_by_address[replica_address] = channel
        self.status_stubs_by_address[replica_address] = stub
        return stub

    def try_get_status(self, replica_address: str, timeout_seconds: float = 0.5):
        try:
            self._log(f"probing replica status at {replica_address}")
            return self.get_status_stub(replica_address).Status(replica_admin_pb2.StatusRequest(), timeout=timeout_seconds)
        except grpc.RpcError:
            self._log(f"status probe to {replica_address} failed")
            return None

    def find_leader_address(self) -> str:
        addresses_to_check: list[str] = []
        if self.cached_leader_address:
            addresses_to_check.append(self.cached_leader_address)
        addresses_to_check.extend([address for address in self.replica_addresses if address != self.cached_leader_address])

        for replica_address in addresses_to_check:
            status_response = self.try_get_status(replica_address)
            if status_response is None:
                continue
            if status_response.role == replica_admin_pb2.LEADER:
                self.cached_leader_address = replica_address
                if self.last_reported_leader_address != replica_address:
                    self._log(
                        f"leader discovery now points to {replica_address} "
                        f"(term={status_response.term}, commit_index={status_response.commit_index})"
                    )
                    self.last_reported_leader_address = replica_address
                return replica_address

        if self.last_reported_leader_address:
            self._log("leader discovery could not find an active leader")
            self.last_reported_leader_address = ""
        self.cached_leader_address = ""
        return ""


class DirectGatewayService(direct_gateway_pb2_grpc.DirectGatewayServicer):
    def __init__(self, replica_directory: GatewayReplicaDirectory):
        self.replica_directory = replica_directory

    def _log(self, message: str) -> None:
        print(f"[gateway] {message}", file=sys.stderr, flush=True)

    def SendDirect(self, request, context):
        self._log(
            f"SendDirect from={request.from_user} to={request.to_user} "
            f"client_message={request.client_id}:{request.client_msg_id}"
        )
        leader_address_hint = self.replica_directory.find_leader_address()
        attempted_addresses: set[str] = set()

        for _ in range(4):
            target_address = leader_address_hint or self.replica_directory.find_leader_address()
            if not target_address or target_address in attempted_addresses:
                break

            attempted_addresses.add(target_address)
            self._log(f"routing write attempt to leader candidate {target_address}")
            try:
                write_reply = self.replica_directory.get_transport_client(target_address).client_write(
                    ClientWriteMessage(
                        from_user=request.from_user,
                        to_user=request.to_user,
                        client_id=request.client_id,
                        client_msg_id=request.client_msg_id,
                        text=request.text,
                    ),
                    timeout_seconds=4.0,
                )
            except grpc.RpcError:
                self._log(f"write attempt to {target_address} failed at transport level")
                leader_address_hint = ""
                continue

            if write_reply.accepted:
                self.replica_directory.cached_leader_address = target_address
                self._log(f"write committed by {target_address} at seq {write_reply.seq}")
                return direct_gateway_pb2.SendDirectResponse(seq=write_reply.seq)

            self._log(
                f"write rejected by {target_address}; leader_hint={write_reply.leader_hint or 'unknown'} "
                f"error={write_reply.error_message or 'none'}"
            )
            leader_address_hint = write_reply.leader_hint

        self._log("write failed because no leader with quorum was reachable")
        context.abort(grpc.StatusCode.FAILED_PRECONDITION, "write could not be committed because no leader with quorum was available")

    def GetConversationHistory(self, request, context):
        read_preference_name = direct_gateway_pb2.ReadPreference.Name(request.read_pref)
        self._log(
            f"GetConversationHistory users=({request.user_a}, {request.user_b}) "
            f"after_seq={request.after_seq} limit={request.limit} read_pref={read_preference_name}"
        )
        candidate_addresses: list[str] = []

        if request.read_pref == direct_gateway_pb2.LEADER_ONLY:
            leader_address = self.replica_directory.find_leader_address()
            if leader_address:
                candidate_addresses.append(leader_address)
        elif request.read_pref == direct_gateway_pb2.REPLICA_HINT and request.replica_hint:
            candidate_addresses.append(request.replica_hint)
            leader_address = self.replica_directory.find_leader_address()
            if leader_address and leader_address != request.replica_hint:
                candidate_addresses.append(leader_address)
        else:
            candidate_addresses.extend(self.replica_directory.replica_addresses)

        attempted_addresses: set[str] = set()
        last_error_message = "no replica could satisfy the read"

        for candidate_address in candidate_addresses:
            if not candidate_address or candidate_address in attempted_addresses:
                continue
            attempted_addresses.add(candidate_address)
            self._log(f"routing read attempt to {candidate_address}")

            try:
                read_reply = self.replica_directory.get_transport_client(candidate_address).read_conversation(
                    ReadConversationMessage(
                        user_a=request.user_a,
                        user_b=request.user_b,
                        after_seq=request.after_seq,
                        limit=request.limit,
                    ),
                    timeout_seconds=3.0,
                )
            except grpc.RpcError:
                self._log(f"read attempt to {candidate_address} failed at transport level")
                continue

            if not read_reply.accepted:
                last_error_message = read_reply.error_message or last_error_message
                self._log(
                    f"read rejected by {candidate_address}; leader_hint={read_reply.leader_hint or 'unknown'} "
                    f"error={last_error_message}"
                )
                continue

            response = direct_gateway_pb2.GetConversationHistoryResponse(served_by=read_reply.served_by)
            for event_dictionary in read_reply.events:
                response.events.append(
                    direct_gateway_pb2.DirectEvent(
                        seq=event_dictionary["seq"],
                        from_user=event_dictionary["from_user"],
                        text=event_dictionary["text"],
                        server_time_ms=event_dictionary["server_time_ms"],
                        client_id=event_dictionary["client_id"],
                        client_msg_id=event_dictionary["client_msg_id"],
                    )
                )
            self._log(
                f"read served by {candidate_address} with {len(response.events)} event(s)"
            )
            return response

        error_code = grpc.StatusCode.UNAVAILABLE if request.read_pref == direct_gateway_pb2.LEADER_ONLY else grpc.StatusCode.FAILED_PRECONDITION
        self._log(f"read failed with error={last_error_message}")
        context.abort(error_code, last_error_message)


def build_argument_parser() -> argparse.ArgumentParser:
    argument_parser = argparse.ArgumentParser(description="Run the direct messaging gateway used by the tests.")
    argument_parser.add_argument("--host", default="127.0.0.1")
    argument_parser.add_argument("--port", type=int, default=DEFAULT_GATEWAY_PORT)
    argument_parser.add_argument("--replica-count", type=int, default=DEFAULT_REPLICA_COUNT)
    argument_parser.add_argument("--replica-start-port", type=int, default=DEFAULT_REPLICA_START_PORT)
    return argument_parser


def main() -> int:
    parsed_arguments = build_argument_parser().parse_args()
    bind_address = f"0.0.0.0:{parsed_arguments.port}"
    replica_addresses = infer_replica_addresses(
        host=parsed_arguments.host,
        replica_count=parsed_arguments.replica_count,
        replica_start_port=parsed_arguments.replica_start_port,
    )

    replica_directory = GatewayReplicaDirectory(replica_addresses)
    print(
        f"[gateway] starting on {bind_address} with replicas={replica_addresses}",
        file=sys.stderr,
        flush=True,
    )
    grpc_server = None
    last_bind_error: Exception | None = None
    for _ in range(25):
        try:
            grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=32))
            direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(DirectGatewayService(replica_directory), grpc_server)
            grpc_server.add_insecure_port(bind_address)
            grpc_server.start()
            last_bind_error = None
            print(f"[gateway] listening on {bind_address}", file=sys.stderr, flush=True)
            break
        except RuntimeError as bind_error:
            last_bind_error = bind_error
            if grpc_server is not None:
                grpc_server.stop(grace=0)
            time.sleep(0.2)

    if grpc_server is None or last_bind_error is not None:
        raise last_bind_error if last_bind_error is not None else RuntimeError(f"could not bind gateway server to {bind_address}")

    try:
        grpc_server.wait_for_termination()
    except KeyboardInterrupt:
        print("[gateway] received KeyboardInterrupt, shutting down", file=sys.stderr, flush=True)
    finally:
        replica_directory.close()
        grpc_server.stop(grace=None)
        print("[gateway] stopped", file=sys.stderr, flush=True)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
