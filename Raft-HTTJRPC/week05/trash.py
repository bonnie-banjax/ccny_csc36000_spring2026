
# various stages of unnecessay from direct_gateway

# BEGIN debug

# async def async_cluster_sanity_check(stubs): # BEGIN
#   """
#   Consolidated async check:
#   1. Waits 2 seconds for cluster spin-up.
#   2. Pings all nodes simultaneously with a 2s timeout.
#   3. Reports total ready count and specific roles.
#   4. Identifies if a Leader exists.
#   """
#
#   sys.stderr.write("\n[GATEWAY_LOG] Waiting 2s for cluster spin-up...\n")
#   await asyncio.sleep(2) # Non-blocking sleep
#
#   sys.stderr.write("[GATEWAY_LOG] --- Starting Parallel Replica Sanity Check ---\n")
#
#   # Create a list of coroutines for all status calls
#   tasks = []
#   for s in stubs:
#     tasks.append(s['stub'].Status(replica_admin_pb2.StatusRequest(), timeout=2.0))
#
#   results = await asyncio.gather(*tasks, return_exceptions=True)
#
#   ready_count = 0
#   leader_found = None
#
#   for i, resp in enumerate(results):
#     s = stubs[i]
#     if isinstance(resp, Exception):
#       sys.stderr.write(f"[GATEWAY_LOG] ID {s['id']} at {s['addr']} FAILED: {str(resp)}\n")
#     else:
#       ready_count += 1
#       role_name = replica_admin_pb2.Role.Name(resp.role)       # Access role by name or integer mapping
#       sys.stderr.write(f"[GATEWAY_LOG] ID: {resp.id} | Role: {role_name} | Term: {resp.term}\n")
#
#       if resp.role == replica_admin_pb2.LEADER or resp.role == 2:
#         leader_found = s['addr']
#
#   sys.stderr.write(f"[GATEWAY_LOG] Readiness Report: {ready_count}/{len(stubs)} nodes reachable.\n")
#
#   if leader_found:
#     sys.stderr.write(f"[GATEWAY_LOG] SUCCESS: Found leader at {leader_found}\n")
#   else:
#     sys.stderr.write("[GATEWAY_LOG] FAILURE: No leader detected via gRPC.\n")
#
#   sys.stderr.write("[GATEWAY_LOG] --- End Sanity Check ---\n\n")
#   return ready_count > 0
# # END
#
# async def debugserve(): # BEGIN
# # class DebugInterceptor(grpc.aio.ServerInterceptor):
# #   async def intercept_service(self, continuation, handler_call_details):
# #     # This will print the EXACT method string the client is requesting
# #     sys.stderr.write(f"[DEBUG] Client is requesting method: {handler_call_details.method}\n")
# #     return await continuation(handler_call_details)
#
#   # 1. Load the cluster info (Replicas)
#   # Ensure this function creates grpc.aio channels!
#   stubs = load_replica_stubs_async()
#
#   stop_event = asyncio.Event()
#   server = grpc.aio.server()
#   # server = grpc.aio.server(interceptors=[DebugInterceptor()])
#
#   # 2. Initialize the Servicer with the cluster stubs
#   # This allows SendDirect to call self.get_leader()
#   servicer = DirectGatewayServicer(stubs)
#
#   # This prints every method the registration function will try to grab
#   # sys.stderr.write(f"\nDEBUG: Servicer has SendDirect: {hasattr(servicer, 'SendDirect')}")
#   # sys.stderr.write(f"\nDEBUG: Servicer has SubscribeConversation: {hasattr(servicer, 'SubscribeConversation')}")
#   pb_grpc.add_DirectGatewayServicer_to_server(servicer, server)
#
#   server.add_insecure_port("[::]:50051")
#
#   # 3. Setup OS Signal Handlers for clean shutdown
#   loop = asyncio.get_running_loop()
#   for sig in (signal.SIGINT, signal.SIGTERM):
#     loop.add_signal_handler(sig, stop_event.set)
#
#   # 4. Optional: Run sanity checks asynchronously
#   # await check_cluster_readiness_async(stubs)
#   await async_cluster_sanity_check(stubs)
#
# # BEGIN
# # 1. Imports required for the manual construction
#   from grpc import (
#       method_handlers_generic_handler,
#       unary_unary_rpc_method_handler,
#       unary_stream_rpc_method_handler
#   )
#
#   # 2. Build the manual method map
#   # Note: We use the 'pb' alias you created from the client proto
#   h = {
#       'SendDirect': unary_unary_rpc_method_handler(
#           servicer.SendDirect,
#           request_deserializer=pb.SendDirectRequest.FromString,
#           response_serializer=pb.SendDirectResponse.SerializeToString,
#       ),
#       'SubscribeConversation': unary_stream_rpc_method_handler(
#           servicer.SubscribeConversation,
#           request_deserializer=pb.SubscribeConversationRequest.FromString,
#           response_serializer=pb.DirectEvent.SerializeToString,
#       ),
#       'GetConversationHistory': unary_unary_rpc_method_handler(
#           servicer.GetConversationHistory,
#           request_deserializer=pb.GetConversationHistoryRequest.FromString,
#           response_serializer=pb.GetConversationHistoryResponse.SerializeToString,
#       )
#   }
#
#   # 3. Register BOTH naming variations to catch any prefix mismatch
#   # This ensures that whether the client asks for /direct.DirectGateway/ or /DirectGateway/, it works.
#   server.add_generic_rpc_handlers((
#       method_handlers_generic_handler('direct.DirectGateway', h),
#   ))
#   server.add_generic_rpc_handlers((
#       method_handlers_generic_handler('DirectGateway', h),
#   ))
#
#   sys.stderr.write("[DEBUG] Manual handlers registered for 'direct.DirectGateway' and 'DirectGateway'\n")
# # END
#
#   await server.start()
#   print("[gateway] Async Server started on port 50051")
#
#   # 5. Wait for the stop signal (Ctrl+C or SIGTERM)
#   await stop_event.wait()
#
#   print("[gateway] Closing active streams (5s grace)...")
#   await server.stop(5)
#   for s in stubs: # During shutdown in serve()
#     await s['channel'].close()
#   print("[gateway] Offline.")
# # END

# BEGIN
  # async def debugSendDirect(self, request, context):
  #   sys.stderr.write(f"[DEBUG] SendDirect: {request.from_user} -> {request.to_user}\n")
  #
  #   # Initialize response variables with defaults to avoid NameErrors
  #   resp_seq = 0
  #   current_time = int(time.time() * 1000)
  #
  #   try:
  #     leader_stub, leader_addr = await self.get_leader()
  #     if not leader_addr:
  #       context.set_code(grpc.StatusCode.UNAVAILABLE)
  #       context.set_details("No leader found in cluster")
  #       return pb.SendDirectResponse(seq=0, server_time_ms=current_time)
  #
  #     # 2. HTTP Forwarding
  #     http_port = int(leader_addr.split(':')[-1]) + 1000
  #     url = f"http://127.0.0.1:{http_port}/client_append"
  #
  #     payload = {
  #       "from_user": request.from_user,
  #       "to_user": request.to_user,
  #       "text": request.text,
  #       "client_id": request.client_id,
  #       "client_msg_id": request.client_msg_id
  #     }
  #
  #     # Use to_thread to keep the async loop spinning
  #     response = await asyncio.to_thread(requests.post, url, json=payload, timeout=5.0)
  #     sys.stderr.write(f"[DEBUG] Replica Response: {response.status_code} - {response.text}\n")
  #     data = response.json()
  #
  #     if data.get("ok"):
  #       resp_seq = int(data["seq"]) # Explicit cast to int!
  #       return pb.SendDirectResponse(seq=resp_seq, server_time_ms=current_time)
  #     else:
  #       context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
  #       context.set_details(data.get("error", "Replica rejected write"))
  #       return pb.SendDirectResponse(seq=0, server_time_ms=current_time)
  #
  #   except Exception as e:
  #     sys.stderr.write(f"[ERROR] SendDirect failed: {str(e)}\n")
  #     context.set_code(grpc.StatusCode.INTERNAL)
  #     # Safe return: we know current_time is defined, and we don't use 'seq'
  #     return pb.SendDirectResponse(seq=0, server_time_ms=current_time)
# END


  # BEGIN send direct
  # async def SendDirect(self, request, context):
  #   sys.stderr.write(f"[DEBUG] SendDirect: {request.from_user} -> {request.to_user}\n")
  #
  #   # Initialize response variables with defaults to avoid NameErrors
  #   resp_seq = 0
  #   current_time = int(time.time() * 1000)
  #
  #   try:
  #     leader_stub, leader_addr = await self.get_leader()
  #     if not leader_addr:
  #       context.set_code(grpc.StatusCode.UNAVAILABLE)
  #       context.set_details("No leader found in cluster")
  #       return pb.SendDirectResponse(seq=0, server_time_ms=current_time)
  #
  #     # 2. HTTP Forwarding
  #     http_port = int(leader_addr.split(':')[-1]) + 1000
  #     url = f"http://127.0.0.1:{http_port}/client_append"
  #
  #     payload = {
  #       "from_user": request.from_user,
  #       "to_user": request.to_user,
  #       "text": request.text,
  #       "client_id": request.client_id,
  #       "client_msg_id": request.client_msg_id
  #     }
  #
  #     # Use to_thread to keep the async loop spinning
  #     response = await asyncio.to_thread(requests.post, url, json=payload, timeout=5.0)
  #     sys.stderr.write(f"[DEBUG] Replica Response: {response.status_code} - {response.text}\n")
  #     data = response.json()
  #
  #     if data.get("ok"):
  #       resp_seq = int(data["seq"]) # Explicit cast to int!
  #       return pb.SendDirectResponse(seq=resp_seq, server_time_ms=current_time)
  #     else:
  #       error_msg = data.get("error", "Unknown error")
  #       # If it's a quorum issue, use UNAVAILABLE.
  #       # If it's a leader issue, use UNAVAILABLE so the client retries.
  #       status = grpc.StatusCode.UNAVAILABLE if "quorum" in error_msg else grpc.StatusCode.INTERNAL
  #       await context.abort(status, error_msg)
  #
  #       # # Check for the specific quorum error string returned by ReplicaAdmin
  #       # error_msg = data.get("error", "")
  #       # if error_msg == "quorum_unavailable":
  #       #   # This triggers the grpc.RpcError that pytest is expecting
  #       #   await context.abort(grpc.StatusCode.UNAVAILABLE, "Raft cluster lacks quorum")
  #       # if error_msg == "not_leader":
  #       #   context.set_details(f"Not leader. Hint: {data.get('leader_hint')}")
  #       #   await context.abort(grpc.StatusCode.INTERNAL, "Redirect to leader")
  #       # await context.abort(grpc.StatusCode.FAILED_PRECONDITION, error_msg)
  #
  #   except Exception as e:
  #     if "Connection" in str(e) or "Timeout" in str(e): # 1. Check if the error suggests the leader is gone
  #       sys.stderr.write(f"[DEBUG] Leader {self.current_leader_addr} failed, re-discovering...\n")
  #       # 2. Trigger get_leader immediately instead of waiting for the background task
  #       new_stub, new_addr = await self.get_leader()
  #       if new_addr:
  #         self.current_leader_addr = new_addr
  #         self.current_leader_http_port = int(new_addr.split(':')[-1]) + 1000
  #       else: # Clear the cache so other RPCs don't keep hitting the dead node
  #         self.current_leader_addr = None
  #     # sys.stderr.write(f"[ERROR] SendDirect failed: {str(e)}\n")
  #     # context.set_code(grpc.StatusCode.INTERNAL)
  #     # # Safe return: we know current_time is defined, and we don't use 'seq'
  #     # return pb.SendDirectResponse(seq=0, server_time_ms=current_time)
  # END

  # async def GetConversationHistory(self, request, context): # BEGIN fake async
  #   """Real implementation: Must fail if cluster is down."""
  #   try:
  #     stub, addr = await self.get_leader()       # Reuse your existing leader discovery
  #     if not addr:
  #       await context.abort(grpc.StatusCode.UNAVAILABLE, "No leader available for read")
  #
  #     # Call the internal HTTP read_history on the leader
  #     http_port = int(addr.split(':')[-1]) + 1000
  #     url = f"http://127.0.0.1:{http_port}/read_history"
  #     payload = {"user_a": request.user_a, "user_b": request.user_b}
  #
  #     # Match the behavior of SendDirect
  #     response = await asyncio.to_thread(requests.post, url, json=payload, timeout=2.0)
  #     data = response.json()
  #
  #     if not data.get("ok"):         # This is critical for the test_quorum_failure
  #       await context.abort(grpc.StatusCode.UNAVAILABLE, data.get("error", "Quorum lost"))
  #
  #     # Map internal events to Proto events
  #     proto_events = [
  #       pb.DirectEvent(
  #         seq=int(e["seq"]),
  #         text=e["text"],
  #         from_user=e["from_user"],
  #         server_time_ms=int(e["server_time_ms"])
  #       ) for e in data.get("events", [])
  #     ]
  #     return pb.GetConversationHistoryResponse(events=proto_events, served_by=[addr])
  #   except grpc.RpcError:
  #     raise # Re-raise gRPC specific errors (like the abort above)
  #   except Exception as e:
  #     sys.stderr.write(f"[ERROR] History failed: {str(e)}\n")
  #     await context.abort(grpc.StatusCode.UNAVAILABLE, "Cluster communication error")
  # END

# BEGIN SubscribeConversation
  # async def SubscribeConversation(self, request, context):
  #   last_seq = request.after_seq
  #
  #   while True:
  #     # 1. Non-blocking check of the cache
  #     addr = self.current_leader_addr
  #     port = self.current_leader_http_port
  #
  #     if not addr:
  #       sys.stderr.write("[DEBUG] No cached leader, skipping poll...\n")
  #       await asyncio.sleep(2)
  #       continue
  #
  #     # 2. Fire the request
  #     try:
  #       # Note: Using POST to be safe with body-data requirements
  #       url = f"http://127.0.0.1:{port}/read_history"
  #       payload = {
  #         "user_a": request.user_a,
  #         "user_b": request.user_b,
  #         "after_seq": last_seq
  #       }
  #
  #       # Using a very aggressive timeout for the poll
  #       response = await asyncio.to_thread(
  #         requests.post, url, json=payload, timeout=1.5
  #       )
  #
  #       if response.status_code == 200:
  #         data = response.json()
  #         for e_data in data.get("events", []):
  #           event = pb.DirectEvent(
  #             seq=int(e_data["seq"]),
  #             text=e_data["text"],
  #             from_user=e_data["from_user"],
  #             server_time_ms=int(e_data.get("server_time_ms", 0))
  #           )
  #           yield event
  #           last_seq = max(last_seq, event.seq)
  #
  #     except Exception as e:
  #       if "Connection" in str(e) or "Timeout" in str(e): # 1. Check if the error suggests the leader is gone
  #         sys.stderr.write(f"[DEBUG] Leader {self.current_leader_addr} failed, re-discovering...\n")
  #         # 2. Trigger get_leader immediately instead of waiting for the background task
  #         new_stub, new_addr = await self.get_leader()
  #         if new_addr:
  #           self.current_leader_addr = new_addr
  #           self.current_leader_http_port = int(new_addr.split(':')[-1]) + 1000
  #         else: # Clear the cache so other RPCs don't keep hitting the dead node
  #           self.current_leader_addr = None
  #
  #     await asyncio.sleep(1.0)
# END

  # BEGIN
    # asyncio.create_task(self._maintain_leader_cache())

  # async def _maintain_leader_cache(self):
  #       """Background loop: Never blocks the main RPCs"""
  #       while True:
  #           try:
  #               # Use a very short timeout so we don't hang
  #               stub, addr = await asyncio.wait_for(self.get_leader(), timeout=2.0)
  #               self.current_leader_addr = addr
  #               self.current_leader_http_port = int(addr.split(':')[-1]) + 1000
  #           except Exception:
  #               # If discovery fails, keep the old one or set to None
  #               pass
  #           await asyncio.sleep(5) # Only check every 5 seconds
  # # END

# END serves
####

# BEGIN cruft

# if __name__ == "__main__":
#   serve()

# BEGIN sync versions (scattered outdated)
# def check_cluster_readiness(stubs):
#     import time
#     sys.stderr.write("[CHECKER] Waiting for replicas to become reachable...\n")
#     for _ in range(5):  # Try 5 times
#         for s in stubs:
#             try:
#                 s['stub'].Status(replica_admin_pb2.StatusRequest(), timeout=0.5)
#                 sys.stderr.write(f"[CHECKER] Replica {s['id']} is UP.\n")
#                 return True # If even one is up, we're getting somewhere
#             except Exception:
#                 continue
#         time.sleep(1) # Wait before retrying
#     return False

# def sanity_check_replicas(stubs):
#
#   # BEGIN ensures replicas have enough time to spin up
#   import time
#   time.sleep(2) # Wait before retrying
#   sys.stderr.write("\nScanning cluster for leader...")
#
#   test_gateway = DirectGatewayServicer(stubs)
#   leader_stub, leader_addr = test_gateway.get_leader()
#   sys.stderr.write(f"\nDEBUG: Found leader at {leader_addr}")
#   if leader_addr:
#       sys.stderr.write(f"\nSUCCESS: Found leader at {leader_addr}")
#   else:
#       sys.stderr.write("\nFAILURE: No leader detected via gRPC. Checking HTTP fallback...")
#   # END
#
#   # BEGIN enumerates replicas & their roles
#   sys.stderr.write("\n[GATEWAY_LOG] --- Starting Replica Sanity Check ---\n")
#   for s in stubs:
#     try:
#       # Note the use of replica_admin_pb2.StatusRequest()
#       resp = s['stub'].Status(replica_admin_pb2.StatusRequest(), timeout=2.0)
#       # Access role by name from the enum if it's an int
#       role_name = replica_admin_pb2.Role.Name(resp.role)
#       sys.stderr.write(f"[GATEWAY_LOG] ID: {resp.id} | Role: {role_name} | Term: {resp.term}\n")
#     except Exception as e:
#       sys.stderr.write(f"[GATEWAY_LOG] ID {s['id']} at {s['addr']} FAILED: {str(e)}\n")
#   sys.stderr.write("[GATEWAY_LOG] --- End Sanity Check ---\n\n")
  # END
# END

# async def serve(): # BEGIN
#   stop_event = asyncio.Event()
#   server = grpc.aio.server()
#   pb_grpc.add_DirectGatewayServicer_to_server(DirectGateway(), server)
#   server.add_insecure_port("[::]:50051")
#
#   loop = asyncio.get_running_loop()
#   for sig in (signal.SIGINT, signal.SIGTERM):
#    loop.add_signal_handler(sig, stop_event.set)
#
#   await server.start()
#
#   await stop_event.wait() # await console_task revealed sys wasn't imported
#   print("[gateway] Closing active streams (5s grace)...")
#   await server.stop(5)
#   console_task.cancel()
#   print("[gateway] Offline.")
# END

# def serve(): # BEGIN
#   stubs = load_replica_stubs()
#
#   server = grpc.server(ThreadPoolExecutor(max_workers=10))
#   direct_gateway_pb2_grpc.add_DirectGatewayServicer_to_server(
#     DirectGatewayServicer(stubs), server)
#   server.add_insecure_port('[::]:50051')
#   server.start()
#   print("DirectGateway gRPC server started on port 50051")
#
#   check_cluster_readiness(stubs)
#   sanity_check_replicas(stubs)
#
#   server.wait_for_termination()
# END

# BEGIN fake async
  # async def SubscribeConversation(self, request, context):
  #     print(f"[GATEWAY] New subscription request: {request.user_a} <-> {request.user_b}")
  #     # Must be an async generator to keep the stream alive
  #     while True:
  #         await asyncio.sleep(10)
  #         yield pb.DirectEvent()

  # async def GetConversationHistory(self, request, context):
  #   # Must return this specific message type to satisfy the await
  #   return pb.GetConversationHistoryResponse(events=[], served_by=["gate-1"])
  #
  # async def SubscribeConversation(self, request, context):
  #   # Must be an async generator to satisfy the 'async for' in client
  #   while True:
  #     await asyncio.sleep(3600) # Just stay open
  #     yield pb.DirectEvent()
  #
  # async def SendDirect(self, request, context):
  #   return pb.SendDirectResponse(seq=1)
# END

# BEGIN original SendDirect & Get GetConversationHistory

  # def SendDirect(self, request, context):
  #   key = (request.client_id, request.client_msg_id)
  #   if key in dedup_store:
  #     return direct_gateway_pb2.SendDirectResponse(seq=dedup_store[key])
  #
  #   # Dynamically find the leader
  #   leader_stub, leader_addr = self.get_leader()
  #
  #   if not leader_stub:
  #     context.set_code(grpc.StatusCode.UNAVAILABLE)
  #     context.set_details("No leader found in cluster.")
  #     return direct_gateway_pb2.SendDirectResponse()
  #
  #   # Now you can use leader_stub to forward the actual request
  #   # seq = leader_stub.AppendEntry(...)
  #   seq = 1 # Placeholder for now
  #   dedup_store[key] = seq
  #   return direct_gateway_pb2.SendDirectResponse(seq=seq)

  # def GetConversationHistory(self, request, context):
  #   replica_stub = self.replica_stubs[0]['stub']
  #   events = []
  #   served_by = [self.replica_stubs[0]['addr']]
  #   return direct_gateway_pb2.GetConversationHistoryResponse(events=events, served_by=served_by)

# END

# BEGIN sync version
  # def get_leader(self):
  #   """
  #   Scans the cluster stubs and returns the one currently acting as LEADER.
  #   """
  #   for replica in self.replica_stubs:
  #     try:
  #       # Attempt to get status from the replica
  #       status = replica['stub'].Status(replica_admin_pb2.StatusRequest(), timeout=1.0)
  #       # Check if this node is the leader
  #       # Note: We check against the enum name 'LEADER'
  #       if status.role == replica_admin_pb2.LEADER or status.role == 2:
  #         return replica['stub'], replica['addr']
  #     except Exception:
  #       # If a node is down, skip to the next one
  #       continue
  #   return None, None
# END sync version

# BEGIN no idea if this was better or something
# import requests
#
  # def GetConversationHistory(self, request, context):
  #   # 1. Pick a replica (using the first one for simplicity,
  #   # or you could use your get_leader logic)
  #   target_addr = self.replica_stubs[0]['addr']
  #   # Map the gRPC port (e.g. 50061) to the HTTP port (e.g. 51061)
  #   # if they differ by a fixed offset
  #   http_port = int(target_addr.split(':')[-1]) + 1000
  #   url = f"http://127.0.0.1:{http_port}/read_history"
  #
  #   # 2. Prepare the JSON payload matches your handle_read_history keys
  #   payload = {
  #     "user_a": request.user_a,
  #     "user_b": request.user_b,
  #     "after_seq": request.after_seq,
  #     "limit": request.limit
  #   }
  #
  #   try:
  #     response = requests.post(url, json=payload, timeout=5.0)
  #     data = response.json()
  #
  #     if not data.get("ok"):
  #       context.set_details("Replica returned error")
  #       context.set_code(grpc.StatusCode.INTERNAL)
  #       return direct_gateway_pb2.GetConversationHistoryResponse()
  #
  #     # 3. Convert JSON dicts into Protobuf objects
  #     proto_events = []
  #     for e in data["events"]:
  #       event = direct_gateway_pb2.DirectEvent(
  #         seq=e["seq"],
  #         from_user=e["from_user"],
  #         text=e["text"],
  #         server_time_ms=e["server_time_ms"]
  #       )
  #       proto_events.append(event)
  #
  #     # 4. Return the gRPC response object
  #     return direct_gateway_pb2.GetConversationHistoryResponse(
  #       events=proto_events,
  #       served_by=data["served_by"]
  #     )
  #
  #   except Exception as e:
  #     context.set_details(f"Gateway failed to reach replica: {str(e)}")
  #     context.set_code(grpc.StatusCode.UNAVAILABLE)
  #     return direct_gateway_pb2.GetConversationHistoryResponse()
# END

# BEGIN previous load stubs
# def load_replica_stubs(cluster_json_path=None):
#   """Load replica addresses from cluster.json and create gRPC stubs."""
#   if cluster_json_path is None:
#     cluster_json_path = os.path.join(os.path.dirname(__file__), '.runtime/cluster.json')
#     cluster_json_path = os.path.abspath(cluster_json_path)
#   with open(cluster_json_path, 'r') as f:
#     cluster = json.load(f)
#   stubs = []
#   for replica in cluster.get('replicas', []):
#     addr = replica['addr']
#     channel = grpc.insecure_channel(addr)
#     stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
#     stubs.append({'id': replica['id'], 'addr': addr, 'stub': stub})
#   return stubs

# def load_replica_stubs(cluster_json_path=None):
#   if cluster_json_path is None:
#     cluster_json_path = os.path.join(os.path.dirname(__file__), '.runtime/cluster.json')
#
#   cluster_json_path = os.path.abspath(cluster_json_path)
#   # LOG PRINT 1: Path Verification
#   sys.stderr.write(f"[STUB_LOADER] Looking for cluster.json at: {cluster_json_path}\n")
#
#   if not os.path.exists(cluster_json_path):
#     sys.stderr.write(f"[STUB_LOADER] ERROR: File not found!\n")
#     return []
#
#   with open(cluster_json_path, 'r') as f:
#     cluster = json.load(f)
#
#   stubs = []
#   replicas_data = cluster.get('replicas', [])
#
#   # LOG PRINT 2: Data Verification
#   sys.stderr.write(f"[STUB_LOADER] Found {len(replicas_data)} replica entries in JSON.\n")
#
#   for replica in replicas_data:
#     addr = replica['addr']
#     sys.stderr.write(f"[STUB_LOADER] Creating stub for ID {replica['id']} at {addr}\n")
#     channel = grpc.insecure_channel(addr)
#     stub = replica_admin_pb2_grpc.ReplicaAdminStub(channel)
#     stubs.append({'id': replica['id'], 'addr': addr, 'stub': stub})
#
#   sys.stderr.flush()
#   return stubs

# END

# BEGIN
  # async def SendDirect(self, request, context): # BEGIN
  #   key = (request.client_id, request.client_msg_id)
  #
  #   if key in self.dedup_store:
  #     return pb.SendDirectResponse(seq=self.dedup_store[key])
  #
  #   # 1. Await the leader discovery
  #   leader_stub, leader_addr = await self.get_leader()
  #
  #   if not leader_addr:
  #     context.set_code(grpc.StatusCode.UNAVAILABLE)
  #     context.set_details("Leader not found")
  #     return pb.SendDirectResponse()
  #
  #   # 2. Map address to HTTP and forward the message
  #   # Mapping 5006x -> 5106x
  #   http_port = int(leader_addr.split(':')[-1]) + 1000
  #   url = f"http://127.0.0.1:{http_port}/append" # Or your specific append endpoint
  #
  #   payload = {
  #     "from_user": request.from_user,
  #     "to_user": request.to_user,
  #     "text": request.text,
  #     "client_id": request.client_id,
  #     "client_msg_id": request.client_msg_id
  #   }
  #
  #   try:
  #     # Use to_thread so the Gateway doesn't hang during the HTTP POST
  #     response = await asyncio.to_thread(
  #       requests.post, url, json=payload, timeout=5.0
  #     )
  #     data = response.json()
  #
  #     if data.get("ok"):
  #       seq = data["seq"]
  #       self.dedup_store[key] = seq
  #       return pb.SendDirectResponse(seq=seq)
  #
  #   except Exception as e:
  #     context.set_code(grpc.StatusCode.INTERNAL)
  #     return pb.SendDirectResponse(seq=seq, server_time_ms=int(time.time() * 1000))
  # # END

  # async def SendDirect(self, request, context): # BEGIN
  #     key = (request.client_id, request.client_msg_id)
  #
  #     # 0. Deduplication check (now that self.dedup_store is fixed)
  #     if key in self.dedup_store:
  #         return pb.SendDirectResponse(
  #             seq=self.dedup_store[key],
  #             server_time_ms=int(time.time() * 1000)
  #         )
  #
  #     # 1. Await leader discovery
  #     leader_stub, leader_addr = await self.get_leader()
  #
  #     if not leader_addr:
  #         context.set_code(grpc.StatusCode.UNAVAILABLE)
  #         context.set_details("Leader not found")
  #         # Return an empty object; gRPC needs a message instance, not None
  #         return pb.SendDirectResponse()
  #
  #     # 2. Forward to Replica HTTP Append
  #     http_port = int(leader_addr.split(':')[-1]) + 1000
  #     url = f"http://127.0.0.1:{http_port}/append"
  #
  #     payload = {
  #         "from_user": request.from_user,
  #         "to_user": request.to_user,
  #         "text": request.text,
  #         "client_id": request.client_id,
  #         "client_msg_id": request.client_msg_id
  #     }
  #
  #     try:
  #         response = await asyncio.to_thread(
  #             requests.post, url, json=payload, timeout=5.0
  #         )
  #         data = response.json()
  #
  #         if data.get("ok"):
  #             seq = int(data["seq"])
  #             self.dedup_store[key] = seq
  #             return pb.SendDirectResponse(
  #                 seq=seq,
  #                 server_time_ms=int(time.time() * 1000)
  #             )
  #         else:
  #             context.set_code(grpc.StatusCode.INTERNAL)
  #             context.set_details(data.get("error", "Replica rejected append"))
  #             return pb.SendDirectResponse()
  #
  #     except Exception as e:
  #         sys.stderr.write(f"[GATEWAY] Error forwarding to replica: {e}\n")
  #         context.set_code(grpc.StatusCode.INTERNAL)
  #         # Fix: Do not reference 'seq' here as it might not be defined
  #         return pb.SendDirectResponse(server_time_ms=int(time.time() * 1000))

  # async def SendDirect(self, request, context):
  #     # Log that we actually entered the method
  #     sys.stderr.write(f"[DEBUG] Method SendDirect entered for user: {request.from_user}\n")
  #
  #     # Return a hardcoded SUCCESS using the Client's PB definition
  #     # Ensure fields match exactly what the client expects
  #     return pb.SendDirectResponse(
  #         seq=999,
  #         server_time_ms=int(time.time() * 1000)
  #     )
  # END
# END

# BEGIN
  # async def SubscribeConversation(self, request, context):
  #   sys.stderr.write(f"[DEBUG] Subscription started for {request.user_a} vs {request.user_b}\n")
  #
  #   # We start looking for messages after the sequence number the client provided
  #   last_seen_seq = request.after_seq
  #
  #   try:
  #       while True:
  #           # 1. Find the leader to ask for data
  #           sys.stderr.write("[DEBUG] Loop Tick - Looking for leader...\n")
  #
  #
  #           leader_addr = self.current_leader_addr
  #           port = self.current_leader_http_port
  #           sys.stderr.write(f"[DEBUG] Loop Tick - Looking for leader...\n leader {leader_addr} & port {port} \n")
  #           # leader_stub, leader_addr = await self.get_leader()
  #           if not leader_addr:
  #               sys.stderr.write("[DEBUG] No cached leader, skipping poll...\n")
  #               await asyncio.sleep(1)
  #               continue
  #
  #           # 2. Poll the replica's history/events endpoint
  #           # Assuming your replica has an endpoint like /history or /events
  #           http_port = int(leader_addr.split(':')[-1]) + 1000
  #           url = f"http://127.0.0.1:{http_port}/read_history"
  #
  #           params = {
  #               "user_a": request.user_a,
  #               "user_b": request.user_b,
  #               "after_seq": last_seen_seq,
  #               "limit": 10
  #           }
  #
  #           try:
  #               response = await asyncio.to_thread(
  #                   requests.get, url, params=params, timeout=2.0
  #               )
  #               if response.status_code == 200:
  #                   data = response.json()
  #                   events = data.get("events", [])
  #                   sys.stderr.write(f"[DEBUG] Polled {url}, got {len(events)} events for {params['user_a']}\n")
  #                   for event_data in events:
  #                       # Convert the JSON/Dict into a Protobuf DirectEvent
  #                       event = pb.DirectEvent(
  #                           seq=int(event_data["seq"]),
  #                           text=event_data["text"],
  #                           from_user=event_data["from_user"],
  #                           server_time_ms=int(event_data.get("server_time_ms", 0))
  #                           # Add other fields as defined in your client.proto
  #                       )
  #                       yield event
  #                       last_seen_seq = max(last_seen_seq, event.seq)
  #
  #           except Exception as e:
  #               sys.stderr.write(f"[DEBUG] Poll error: {e}\n")
  #
  #           # 3. Wait a bit before polling again to avoid spamming
  #           await asyncio.sleep(0.5)
  #
  #   except asyncio.CancelledError:
  #       sys.stderr.write(f"[DEBUG] Subscription closed for {request.user_a}\n")
# END
# END