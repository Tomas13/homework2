package com.example.grpc;

import com.example.grpc.ping.PingRequest;
import com.example.grpc.ping.PingResponse;
import com.example.grpc.ping.PingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public class PingClient {

    public static void main(String[] args) throws InterruptedException {
        String host = args.length > 0 ? args[0] : "localhost";
        int port = args.length > 1 ? Integer.parseInt(args[1]) : 8080;

        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            PingServiceGrpc.PingServiceBlockingStub stub = PingServiceGrpc.newBlockingStub(channel);

            PingRequest request = PingRequest.newBuilder()
                    .setMessage("Ping")
                    .setClient("java")
                    .build();

            System.out.println("Sending: " + request.getMessage());
            PingResponse response = stub.ping(request);

            System.out.println("Received response:");
            System.out.println("  message:   " + response.getMessage());
            System.out.println("  timestamp: " + response.getTimestamp());
            System.out.println("  server:    " + response.getServer());
        } finally {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
