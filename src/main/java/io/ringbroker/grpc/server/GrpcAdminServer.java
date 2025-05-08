package io.ringbroker.grpc.server;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.ringbroker.grpc.services.TopicAdminServiceImpl;
import io.ringbroker.registry.TopicRegistry;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GrpcAdminServer {

    private final int port;
    private final TopicRegistry topicRegistry;
    private Server server;

    public GrpcAdminServer(int port, TopicRegistry topicRegistry) {
        this.port = port;
        this.topicRegistry = topicRegistry;
    }

    public void start() throws Exception {
        server = ServerBuilder.forPort(port)
                .addService(new TopicAdminServiceImpl(topicRegistry))
                .build()
                .start();

        log.info("gRPC Admin Service started on port {}", port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down gRPC Admin Server...");
            stop();
        }));

        server.awaitTermination();
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }
}
