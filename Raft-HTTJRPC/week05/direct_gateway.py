
# passes "test_messages_read_in_order"
# passes "test_client_retry_is_idempotent"
# passes "test_requests_fail_without_quorum"
# regression on "test_single_leader_per_term_observed"

# BEGIN imports for "base"
import os
import json
import grpc
from concurrent.futures import ThreadPoolExecutor
from generated import replica_admin_pb2_grpc
# from generated import direct_gateway_pb2, direct_gateway_pb2_grpc
# END

# BEGIN merge in from mvp_gateway
import signal
import sys

import asyncio
import time
import grpc
from concurrent import futures

import grpc.aio
import requests
#
from generated import direct_client_pb2 as pb
from generated import direct_client_pb2_grpc as pb_grpc
# import direct_client_pb2_grpc as client_grpc
# END



# BEGIN Connective Tissue

import sys
import replica_admin_pb2
import replica_admin_pb2_grpc



dedup_store = {}

# BEGIN



def load_replica_stubs_async(cluster_json_path=None):
  """Load replica addresses and create async gRPC stubs."""
  if cluster_json_path is None:
    cluster_json_path = os.path.join(os.path.dirname(__file__), '.runtime/cluster.json')

  cluster_json_path = os.path.abspath(cluster_json_path)

  # Reading the file is fine as a sync call unless the file is massive
  with open(cluster_json_path, 'r') as f:
    cluster = json.load(f)

  stubs = []
  for replica in cluster.get('replicas', []):
    addr = replica['addr']

    # TRANSFORMATION: Use the async IO channel
    # This channel is designed to be used with 'await'
    channel = grpc.aio.insecure_channel(addr)

    # The stub itself remains the same class,
    # but it inherits async behavior from the channel
    stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)

    stubs.append({
      'id': replica['id'],
      'addr': addr,
      'stub': stub,
      'channel': channel # Good practice to keep the channel to close it later
    })

  return stubs
# END

class DirectGatewayServicer(pb_grpc.DirectGatewayServicer):
  def __init__(self, replica_stubs):
    self.replica_stubs = replica_stubs
    self.dedup_store = {}
    methods = [m for m in dir(self) if not m.startswith('_')]
    sys.stderr.write(f"[DEBUG] Servicer initialized with methods: {methods}\n")
    self.current_leader_addr = None
    self.current_leader_http_port = None

  async def get_leader(self): # BEGIN
    for replica in self.replica_stubs: # We 'await' the async stub call.
      try: # This allows other tasks to run while waiting for the network.
        status = await replica['stub'].Status(
          replica_admin_pb2.StatusRequest(),
          timeout=1.0
        )
        if status.role == 2 or status.role == replica_admin_pb2.LEADER:
          return replica['stub'], replica['addr']
      except Exception:
        continue
    return None, None
  # END async version

  async def SendDirect(self, request, context): # BEGIN
    sys.stderr.write(f"[DEBUG] SendDirect: {request.from_user} -> {request.to_user}\n")
    current_time = int(time.time() * 1000)
    try:       # 1. Use Cache or Discover
      addr = self.current_leader_addr
      if not addr:
        _, addr = await self.get_leader()
        if addr:
          self.current_leader_addr = addr
          self.current_leader_http_port = int(addr.split(':')[-1]) + 1000
      if not addr:
        await context.abort(grpc.StatusCode.UNAVAILABLE, "No leader found")
      url = f"http://127.0.0.1:{self.current_leader_http_port}/client_append"       # 2. HTTP Forwarding
      payload = {
        "from_user": request.from_user,
        "to_user": request.to_user,
        "text": request.text,
        "client_id": request.client_id,
        "client_msg_id": request.client_msg_id
      }
      response = await asyncio.to_thread(requests.post, url, json=payload, timeout=5.0)
      data = response.json()
      if data.get("ok"):
        return pb.SendDirectResponse(seq=int(data["seq"]), server_time_ms=current_time)
      # 3. Handle Logical Rejections (Quorum/Leadership)
      error_msg = data.get("error", "Unknown error")
      if data.get("error") == "not_leader":
        self.current_leader_addr = None # Invalidate cache
      status = grpc.StatusCode.UNAVAILABLE if "quorum" in error_msg.lower() else grpc.StatusCode.FAILED_PRECONDITION
      await context.abort(status, error_msg)

    except grpc.RpcError:
      raise
    except Exception as e:
      sys.stderr.write(f"[DEBUG] SendDirect transport failure: {e}\n")   # 4. Reactive Cache Invalidation on Network Error
      self.current_leader_addr = None
      status = grpc.StatusCode.UNAVAILABLE if ("Connection" in str(e) or "Timeout" in str(e)) else grpc.StatusCode.INTERNAL
      await context.abort(status, str(e))
  # END

  async def SubscribeConversation(self, request, context): # BEGIN
    last_seq = request.after_seq

    while True:
      # 1. Reactive Leader Check
      if not self.current_leader_addr:
        # If cache is empty, try a fresh discovery immediately
        _, new_addr = await self.get_leader()
        if new_addr:
          self.current_leader_addr = new_addr
          self.current_leader_http_port = int(new_addr.split(':')[-1]) + 1000
        else:
          await asyncio.sleep(1.0)
          continue

      # 2. Attempt the Poll
      try:
        url = f"http://127.0.0.1:{self.current_leader_http_port}/read_history"
        payload = {
          "user_a": request.user_a,
          "user_b": request.user_b,
          "after_seq": last_seq
        }

        # Aggressive timeout to prevent blocking the event loop
        response = await asyncio.to_thread(
          requests.post, url, json=payload, timeout=1.0
        )

        if response.status_code == 200:
          data = response.json()

          # If the replica specifically says it's no longer the leader
          if not data.get("ok") and data.get("error") == "not_leader":
            self.current_leader_addr = None
            continue

          for e_data in data.get("events", []):
            event = pb.DirectEvent(
              seq=int(e_data["seq"]),
              text=e_data["text"],
              from_user=e_data["from_user"],
              server_time_ms=int(e_data.get("server_time_ms", 0))
            )
            yield event
            last_seq = max(last_seq, event.seq)

      except Exception as e:
        # Detect crash/disconnect and invalidate cache immediately
        sys.stderr.write(f"[DEBUG] Subscription poll failed: {e}\n")
        self.current_leader_addr = None
        # Don't yield or return; just loop back to rediscover

      await asyncio.sleep(0.5)
  # END

  async def GetConversationHistory(self, request, context): # BEGIN
    try:
     # 1. Use existing leader if available, else find it
     stub, addr = None, self.current_leader_addr
     if not addr:
      stub, addr = await self.get_leader()
      if addr:
        self.current_leader_addr = addr
        self.current_leader_http_port = int(addr.split(':')[-1]) + 1000

     if not addr:
      await context.abort(grpc.StatusCode.UNAVAILABLE, "No leader found")

     # 2. HTTP Request to the Leader
     url = f"http://127.0.0.1:{self.current_leader_http_port}/read_history"
     payload = {"user_a": request.user_a, "user_b": request.user_b}

     response = await asyncio.to_thread(
      requests.post, url, json=payload, timeout=2.0
     )
     data = response.json()

     # 3. Handle Logical Failures (Quorum/Leadership)
     if not data.get("ok"):
      if data.get("error") == "not_leader":
        self.current_leader_addr = None # Invalidate cache
      await context.abort(grpc.StatusCode.UNAVAILABLE, data.get("error"))

     # 4. Success Path
     proto_events = [
      pb.DirectEvent(
       seq=int(e["seq"]),
       text=e["text"],
       from_user=e["from_user"],
       server_time_ms=int(e.get("server_time_ms", 0))
      ) for e in data.get("events", [])
     ]
     return pb.GetConversationHistoryResponse(
      events=proto_events,
      served_by=[addr]
     )

    except grpc.RpcError:
      raise
    except Exception as e:
      self.current_leader_addr = None # Reset on any transport error
      await context.abort(grpc.StatusCode.UNAVAILABLE, f"Internal Error: {e}")
  # END

async def serve(): # BEGIN
  stubs = load_replica_stubs_async()
  stop_event = asyncio.Event()
  server = grpc.aio.server()
  servicer = DirectGatewayServicer(stubs)
  pb_grpc.add_DirectGatewayServicer_to_server(servicer, server)
  server.add_insecure_port("[::]:50051")

  loop = asyncio.get_running_loop()
  for sig in (signal.SIGINT, signal.SIGTERM):
    loop.add_signal_handler(sig, stop_event.set)

  await server.start()
  print("[gateway] Async Server started on port 50051")
  await stop_event.wait()
  print("[gateway] Closing active streams (5s grace)...")
  await server.stop(1)
  for s in stubs: await s['channel'].close() # During shutdown in serve()
  print("[gateway] Offline.")
# END
if __name__ == "__main__":
  try:
    asyncio.run(serve())
  except KeyboardInterrupt:
    pass


# END


####


# BEGIN old direct_gateway

# passes "test_basic_replication"
# passes "test_follower_failure"

# import os
# import json
# import grpc
# from concurrent.futures import ThreadPoolExecutor
# from generated import replica_admin_pb2_grpc
# from generated import direct_gateway_pb2, direct_gateway_pb2_grpc

# dedup_store = {}
#
# def load_replica_stubs(cluster_json_path=None):
#     """Load replica addresses from cluster.json and create gRPC stubs."""
#     if cluster_json_path is None:
#         cluster_json_path = os.path.join(os.path.dirname(__file__), '.runtime/cluster.json')
#         cluster_json_path = os.path.abspath(cluster_json_path)
#     with open(cluster_json_path, 'r') as f:
#         cluster = json.load(f)
#     stubs = []
#     for replica in cluster.get('replicas', []):
#         addr = replica['addr']
#         channel = grpc.insecure_channel(addr)
#         stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
#         stubs.append({'id': replica['id'], 'addr': addr, 'stub': stub})
#     return stubs
#
# class DirectGatewayServicer(direct_gateway_pb2_grpc.DirectGatewayServicer):
#     def __init__(self, replica_stubs):
#         self.replica_stubs = replica_stubs
#
#     def SendDirect(self, request, context):
#         key = (request.client_id, request.client_msg_id)
#         if key in dedup_store:
#             return direct_gateway_pb2.SendDirectResponse(seq=dedup_store[key])
#         leader_stub = self.replica_stubs[0]['stub']
#         seq = 1  # Placeholder
#         dedup_store[key] = seq
#         return direct_gateway_pb2.SendDirectResponse(seq=seq)
#
#     def GetConversationHistory(self, request, context):
#         replica_stub = self.replica_stubs[0]['stub']
#         events = []
#         served_by = [self.replica_stubs[0]['addr']]
#         return direct_gateway_pb2.GetConversationHistoryResponse(events=events, served_by=served_by)
#
# def serve():
#     stubs = load_replica_stubs()
#     server = grpc.server(ThreadPoolExecutor(max_workers=10))
#     direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(
#         DirectGatewayServicer(stubs), server)
#     server.add_insecure_port('[::]:50051')
#     server.start()
#     print("DirectGateway gRPC server started on port 50051")
#     server.wait_for_termination()
#
# if __name__ == "__main__":
#     serve()


# END
