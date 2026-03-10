
# NOTE: this code is LLM synthesized and yet unreviewed

import asyncio
import time
import grpc
from concurrent import futures

# Assuming generated code is in the 'generated' package as per client imports
from generated import direct_client_pb2 as pb
from generated import direct_client_pb2_grpc as pb_grpc

class DirectGateway(pb_grpc.DirectGatewayServicer):
    def __init__(self):
        self.history = []
        self.cv = asyncio.Condition()
        self.server_seq = 0

    async def SendDirect(self, request, context):
        """Handles incoming messages from the client."""
        async with self.cv:
            self.server_seq += 1
            event = pb.DirectEvent(
                seq=self.server_seq,
                from_user=request.from_user,
                text=request.text,
                server_time_ms=int(time.time() * 1000),
                client_id=request.client_id,
                client_msg_id=request.client_msg_id
            )
            self.history.append(event)
            self.cv.notify_all()

        return pb.SendDirectResponse(seq=self.server_seq)

    async def GetConversationHistory(self, request, context):
        """Returns past messages between two users."""
        # Note: In a real app, you'd filter by (request.user_a, request.user_b)
        # and respect the request.after_seq and limit.
        relevant_events = [
            e for e in self.history
            if e.seq > request.after_seq
        ][-request.limit:]

        return pb.GetConversationHistoryResponse(
            events=relevant_events,
            served_by=["127.0.0.1:50051"]
        )

    async def SubscribeConversation(self, request, context):
        """
        Synthesized RPC to satisfy the direct_client.py requirement.
        Streams new events to the client as they arrive.
        """
        last_sent = request.after_seq

        while True:
            async with self.cv:
                # Wait for new messages if we are caught up
                await self.cv.wait_for(lambda: self.server_seq > last_sent)

                # Fetch new events since the last one we sent
                new_events = [e for e in self.history if e.seq > last_sent]

            for event in new_events:
                if context.done():
                    return
                yield event
                last_sent = event.seq

async def serve():
    server = grpc.aio.server()
    pb_grpc.add_DirectGatewayServicer_to_server(DirectGateway(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    print(f"Gateway starting on {listen_addr}")
    await server.start()
    await server.wait_for_termination()

if __name__ == "__main__":
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass