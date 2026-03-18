#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from dataclasses import dataclass

import grpc

from generated import direct_client_pb2 as pb
from generated import direct_client_pb2_grpc as pb_grpc


@dataclass
class ClientConfig:
    """
    Immutable startup settings for the terminal chat client.

    Inputs:
    - `gateway_addr`: host:port for the gateway process the client will call.
    - `me`: user name for the local participant.
    - `peer`: user name for the remote participant.
    - `client_id`: stable client identifier used for idempotent retries.
    - `history_limit`: maximum number of history events to request on startup.

    Output:
    - An instance is returned by `parse_args()` and then passed into
      `DirectChatClient.__init__()`.

    Called by:
    - Constructed in `parse_args()`.
    - Consumed by `DirectChatClient.__init__()`.
    """
    gateway_addr: str
    me: str
    peer: str
    client_id: str
    history_limit: int = 50


def conv_pair(a: str, b: str) -> tuple[str, str]:
    """
    Return the two participant names in deterministic sorted order.

    Inputs:
    - `a`: one participant identifier.
    - `b`: the other participant identifier.

    Output:
    - A two-element tuple whose lexical ordering is stable, so both sides of a
      conversation compute the same conversation key.

    Called by:
    - `DirectChatClient.__init__()` when it stores `self.user_a` and
      `self.user_b`.
    """
    return (a, b) if a <= b else (b, a)


class DirectChatClient:
    """
    Terminal 1:1 chat client that talks only to a proxy/gateway via gRPC.
    The gateway handles routing to the correct Raft leader and streaming committed events back.
    """

    def __init__(self, cfg: ClientConfig):
        """
        Build the client object that manages the terminal UI and gateway RPCs.

        Inputs:
        - `cfg`: the `ClientConfig` produced by `parse_args()`.

        Output:
        - No return value. The constructor stores configuration, initializes
          stop/dedup state, and computes the canonical conversation pair.

        Called by:
        - `main()` constructs `DirectChatClient(cfg)` before awaiting `run()`.
        """
        self.cfg = cfg
        self._stop = asyncio.Event()
        self._last_seen_seq: int = 0

        # Dedup: avoid printing our own message twice if:
        # (a) we print on ACK and (b) the stream also delivers the same event.
        # This client does NOT print on ACK by default, but we keep this anyway.
        self._recent_client_msg_ids: set[str] = set()
        self._recent_client_msg_ids_max = 200

        self.user_a, self.user_b = conv_pair(cfg.me, cfg.peer)

    async def run(self) -> int:
        """
        Open the gateway channel and run the history, subscribe, and input loops.

        Inputs:
        - No explicit parameters beyond `self`. The method uses the gateway
          address stored in `self.cfg`.

        Output:
        - Returns `0` on clean shutdown and `1` if one of the background tasks
          exits with an exception that should fail the process.

        Called by:
        - `main()` awaits this method as the top-level client workflow.
        """
        async with grpc.aio.insecure_channel(self.cfg.gateway_addr) as channel:
            stub = pb_grpc.DirectGatewayStub(channel)

            await self._catch_up_history(stub)

            sub_task = asyncio.create_task(self._subscribe_loop(stub))
            input_task = asyncio.create_task(self._stdin_loop(stub))

            done, pending = await asyncio.wait(
                {sub_task, input_task},
                return_when=asyncio.FIRST_COMPLETED,
            )

            self._stop.set()
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            for t in done:
                exc = t.exception()
                if exc:
                    print(f"[client] error: {exc}", file=sys.stderr)
                    return 1

        return 0

    async def _catch_up_history(self, stub: pb_grpc.DirectGatewayStub) -> None:
        """
        Fetch recent committed history before the live subscription starts.

        Inputs:
        - `stub`: the gateway gRPC stub created in `run()`.

        Output:
        - No return value. The method prints history to stdout and advances
          `self._last_seen_seq` so later streaming resumes from the newest event.

        Called by:
        - `run()` immediately after it creates the gateway stub.
        """
        try:
            resp = await stub.GetConversationHistory(
                pb.GetConversationHistoryRequest(
                    user_a=self.user_a,
                    user_b=self.user_b,
                    after_seq=0,
                    limit=self.cfg.history_limit,
                ),
                timeout=5.0,
            )
            if resp.events:
                print(f"[client] history (last {len(resp.events)}):")
                for ev in resp.events:
                    self._print_event(ev)
                    self._last_seen_seq = max(self._last_seen_seq, int(ev.seq))
                print("[client] --- live ---")
            else:
                print("[client] --- live ---")
        except grpc.aio.AioRpcError:
            print("[client] (no history available) --- live ---")

    async def _subscribe_loop(self, stub: pb_grpc.DirectGatewayStub) -> None:
        """
        Keep a streaming subscription open and reconnect if the stream breaks.

        Inputs:
        - `stub`: the gateway gRPC stub created in `run()`.

        Output:
        - No direct return value. The method runs until `self._stop` is set,
          printing newly streamed events and updating `self._last_seen_seq`.

        Called by:
        - `run()` via `asyncio.create_task(self._subscribe_loop(stub))`.
        """
        backoff = 0.25
        max_backoff = 3.0

        while not self._stop.is_set():
            req = pb.SubscribeConversationRequest(
                user_a=self.user_a,
                user_b=self.user_b,
                after_seq=max(0, self._last_seen_seq),
            )
            try:
                stream = stub.SubscribeConversation(req, timeout=None)
                async for ev in stream:
                    if self._stop.is_set():
                        break

                    # Dedup our own messages if needed
                    if ev.client_id == self.cfg.client_id and ev.client_msg_id in self._recent_client_msg_ids:
                        self._last_seen_seq = max(self._last_seen_seq, int(ev.seq))
                        continue

                    self._print_event(ev)
                    self._last_seen_seq = max(self._last_seen_seq, int(ev.seq))

                backoff = 0.25
            except grpc.aio.AioRpcError as e:
                if self._stop.is_set():
                    return
                print(f"[client] subscribe error: {e.code().name} {e.details()} (reconnecting...)", file=sys.stderr)
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)

    async def _stdin_loop(self, stub: pb_grpc.DirectGatewayStub) -> None:
        """
        Read terminal input and convert user lines into gateway write RPCs.

        Inputs:
        - `stub`: the gateway gRPC stub created in `run()`.

        Output:
        - No return value. The method exits when EOF or a quit command is seen,
          and it forwards ordinary text lines to `_send_direct()`.

        Called by:
        - `run()` via `asyncio.create_task(self._stdin_loop(stub))`.
        """
        print(f"[client] chatting as '{self.cfg.me}' with '{self.cfg.peer}'")
        print("[client] type messages + Enter. Commands: /help, /quit")
        while not self._stop.is_set():
            line = await asyncio.to_thread(sys.stdin.readline)
            if line == "":  # EOF
                self._stop.set()
                return

            msg = line.strip()
            if not msg:
                continue

            if msg in ("/quit", "/exit"):
                self._stop.set()
                return

            if msg == "/help":
                print("Commands: /help, /quit")
                continue

            await self._send_direct(stub, msg)

    async def _send_direct(self, stub: pb_grpc.DirectGatewayStub, text: str) -> None:
        """
        Send one direct message to the gateway using a fresh client message id.

        Inputs:
        - `stub`: the gateway gRPC stub created in `run()`.
        - `text`: the user-entered message body to send.

        Output:
        - No return value. On success the method updates `self._last_seen_seq`
          from the returned sequence number; on failure it prints an error.

        Called by:
        - `_stdin_loop()` for each non-command line typed by the user.
        """
        client_msg_id = str(uuid.uuid4())
        self._remember_client_msg_id(client_msg_id)

        req = pb.SendDirectRequest(
            from_user=self.cfg.me,
            to_user=self.cfg.peer,
            client_id=self.cfg.client_id,
            client_msg_id=client_msg_id,
            text=text,
        )

        try:
            resp = await stub.SendDirect(req, timeout=5.0)
            # We rely on the stream for display. But we can keep seq in case stream is delayed.
            self._last_seen_seq = max(self._last_seen_seq, int(resp.seq))
        except grpc.aio.AioRpcError as e:
            print(f"[client] send failed: {e.code().name} {e.details()}", file=sys.stderr)

    def _remember_client_msg_id(self, client_msg_id: str) -> None:
        """
        Track recently sent client message ids to avoid duplicate local display.

        Inputs:
        - `client_msg_id`: the UUID assigned in `_send_direct()`.

        Output:
        - No return value. The method mutates the bounded recent-id set.

        Called by:
        - `_send_direct()` before the outbound write RPC is issued.
        """
        self._recent_client_msg_ids.add(client_msg_id)
        if len(self._recent_client_msg_ids) > self._recent_client_msg_ids_max:
            # Drop arbitrary elements to keep bounded
            for _ in range(len(self._recent_client_msg_ids) - self._recent_client_msg_ids_max):
                self._recent_client_msg_ids.pop()

    def _print_event(self, ev: pb.DirectEvent) -> None:
        """
        Render one committed chat event in a human-readable terminal format.

        Inputs:
        - `ev`: one event returned by history or stream RPCs from the gateway.

        Output:
        - No return value. The method prints a formatted line to stdout.

        Called by:
        - `_catch_up_history()` for startup history replay.
        - `_subscribe_loop()` for each streamed event that survives dedup.
        """
        ts = ""
        if ev.server_time_ms:
            ts = time.strftime("%H:%M:%S", time.localtime(ev.server_time_ms / 1000)) + " "

        who = "me" if ev.from_user == self.cfg.me else ev.from_user
        print(f"{ts}[{ev.seq}] {who}: {ev.text}")


def parse_args() -> ClientConfig:
    """
    Parse command-line flags and translate them into `ClientConfig`.

    Inputs:
    - No explicit Python arguments. The function reads process CLI arguments.

    Output:
    - Returns a populated `ClientConfig`, generating a UUID client id if one was
      not provided explicitly.

    Called by:
    - `main()` before it constructs the `DirectChatClient`.
    """
    ap = argparse.ArgumentParser(description="1:1 gRPC Chat Client (talks to gateway/proxy)")
    ap.add_argument("--gateway", required=True, help="Gateway address host:port (insecure).")
    ap.add_argument("--me", required=True, help="My user id / nickname.")
    ap.add_argument("--peer", required=True, help="Peer user id / nickname.")
    ap.add_argument("--client-id", default=None, help="Stable client id for retries (defaults to a UUID).")
    ap.add_argument("--history", type=int, default=50, help="History events to fetch on start.")
    args = ap.parse_args()

    return ClientConfig(
        gateway_addr=args.gateway,
        me=args.me,
        peer=args.peer,
        client_id=args.client_id or str(uuid.uuid4()),
        history_limit=args.history,
    )


async def main() -> int:
    """
    Entry point for the async terminal client process.

    Inputs:
    - No explicit parameters. The function delegates to `parse_args()` for CLI
      input.

    Output:
    - Returns the integer exit code produced by `DirectChatClient.run()`.

    Called by:
    - The module-level `if __name__ == "__main__"` block via `asyncio.run(main())`.
    """
    cfg = parse_args()
    client = DirectChatClient(cfg)
    return await client.run()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        raise SystemExit(0)
