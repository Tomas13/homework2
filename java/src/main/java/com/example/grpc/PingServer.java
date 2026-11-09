package com.example.grpc;

import com.example.grpc.ping.PingRequest;
import com.example.grpc.ping.PingResponse;
import com.example.grpc.ping.PingServiceGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class PingServer {

    private static final int PORT = 8080;
    private static final String SERVER_NAME = "java-ping-server";

    static class PingServiceImpl extends PingServiceGrpc.PingServiceImplBase {
        @Override
        public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
            System.out.printf("Received request: message=\"%s\", client=\"%s\"%n",
                    request.getMessage(), request.getClient());

            String reply = "Ping".equalsIgnoreCase(request.getMessage())
                    ? "Pong"
                    : "Unknown: " + request.getMessage();

            PingResponse response = PingResponse.newBuilder()
                    .setMessage(reply)
                    .setTimestamp(System.currentTimeMillis())
                    .setServer(SERVER_NAME)
                    .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Server server = ServerBuilder.forPort(PORT)
                .addService(new PingServiceImpl())
                .build()
                .start();

        System.out.println("gRPC server started, listening on port " + PORT);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down gRPC server...");
            try {
                server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                server.shutdownNow();
            }
        }));

        server.awaitTermination();
    }
}
