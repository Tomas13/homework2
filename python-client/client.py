"""Simple Python gRPC client for the PingService."""
import sys

import grpc

import ping_pb2
import ping_pb2_grpc


def main() -> None:
    host = sys.argv[1] if len(sys.argv) > 1 else "localhost"
    port = sys.argv[2] if len(sys.argv) > 2 else "8080"

    with grpc.insecure_channel(f"{host}:{port}") as channel:
        stub = ping_pb2_grpc.PingServiceStub(channel)
        request = ping_pb2.PingRequest(message="Ping", client="python")
        print(f"Sending: {request.message}")
        response = stub.Ping(request, timeout=5)
        print("Received response:")
        print(f"  message:   {response.message}")
        print(f"  timestamp: {response.timestamp}")
        print(f"  server:    {response.server}")


if __name__ == "__main__":
    main()
