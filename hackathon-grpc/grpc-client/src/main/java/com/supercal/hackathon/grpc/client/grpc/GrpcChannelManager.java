package com.supercal.hackathon.grpc.client.grpc;

import com.supercal.hackathon.grpc.client.GrpcClientConfig;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueSocketChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class GrpcChannelManager {

    private final List<GrpcConnection> connections = new ArrayList<>();

    public GrpcChannelManager(GrpcClientConfig config) {
        // Executors
        for(int i = 0; i < config.getChannels(); i++) {
            ExecutorService callbackExecutor = Executors.newFixedThreadPool(config.getCallbackThreads());

            // Create a channel and stub
            NettyChannelBuilder builder = NettyChannelBuilder.forAddress(config.getHost(), config.getPort())
                    .usePlaintext()
                    .maxInboundMessageSize(config.getMaxInboundMessageSize() * 1024 * 1024)
                    .flowControlWindow(config.getFlowControlWindow() * 1024 * 1024)
                    .executor(callbackExecutor);

            // IO_uring
            if(config.isIoUring()) {
                log.info("Using IOUring");

                if(IOUring.isAvailable()) {
                    builder
                            .eventLoopGroup(new IOUringEventLoopGroup())
                            .channelType(IOUringSocketChannel.class);
                } else if(KQueue.isAvailable()) {
                    System.out.println("KQueue");
                    builder
                            .eventLoopGroup(new KQueueEventLoopGroup())
                            .channelType(KQueueSocketChannel.class);
                }
            }

            // Create channel
            ManagedChannel channel = builder.build();
            GrpcConnection connection = new GrpcConnection(channel);
            connections.add(connection);
        }
    }

    public void shutdown() {
        connections.forEach(GrpcConnection::shutdown);
    }

    public GrpcConnection getConnection() {
        int index = ThreadLocalRandom.current().nextInt(connections.size());
        return connections.get(index);
    }
}
