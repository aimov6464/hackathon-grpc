package com.supercal.hackathon.grpc.client;

import lombok.Data;

import java.util.Properties;

@Data
public class GrpcClientConfig {

    private final Properties properties;

    // Connection
    private String host;
    private int port;

    // Channel
    private int channels;
    private int callbackThreads;
    private int maxInboundMessageSize;
    private int flowControlWindow;
    private int sendTimeout;
    private String compression;
    private boolean ioUring;

    // Batch
    private int batchSize;
    private int requestInFlight;
    private int tps;

    // Account
    private int accounts;
    private int amount;

    // Partition
    private int partitionsCount;

    // Phase 1
    private int phase1Sleep;
    private int phase1TpsStart;
    private int phase1TpsEnd;
    private int phase1TpsIncrement;

    // Phase 2
    private int phase2Sleep;
    private int phase2TpsStart;

    // Phase 3
    private int phase3Sleep;
    private int phase3TpsStart;
    private int phase3TpsIncrement;

    // Test
    private int simulateDuplicate;

    public GrpcClientConfig(Properties properties) {
        this.properties = properties;

        // Server
        this.host = properties.getProperty("server.host", "localhost");
        this.port = Integer.parseInt(properties.getProperty("server.port", "50051"));

        // Channel
        this.channels = Integer.parseInt(properties.getProperty("channel.count", "3"));
        this.callbackThreads = Integer.parseInt(properties.getProperty("channel.callback.threads", "4"));
        this.maxInboundMessageSize = Integer.parseInt(properties.getProperty("channel.max.inbound.message.size", "1"));
        this.flowControlWindow = Integer.parseInt(properties.getProperty("channel.flow.control.window", "4"));
        this.sendTimeout = Integer.parseInt(properties.getProperty("channel.send.timeout", "5000"));
        this.compression = properties.getProperty("channel.compression");
        this.ioUring = Boolean.parseBoolean(properties.getProperty("channel.iouring", "true"));

        // Batch
        this.batchSize = Integer.parseInt(properties.getProperty("batch.size", "500"));

        // Flow control
        this.tps = Integer.parseInt(properties.getProperty("request.tps.max", "700000"));

        // Account
        this.accounts = Integer.parseInt(properties.getProperty("account.count", "100000"));
        this.amount = Integer.parseInt(properties.getProperty("account.transfer.amount", "1"));

        // Partition
        this.partitionsCount = Integer.parseInt(properties.getProperty("partitions.count", "10"));

        // Test
        this.simulateDuplicate = Integer.parseInt(properties.getProperty("simulate.duplicate.percent", "0"));

        // Phase 1
        this.phase1Sleep = Integer.parseInt(properties.getProperty("phase1.sleep", "60"));
        this.phase1TpsStart = Integer.parseInt(properties.getProperty("phase1.tps.start", "500000"));
        this.phase1TpsEnd = Integer.parseInt(properties.getProperty("phase1.tps.end", "1000000"));
        this.phase1TpsIncrement = Integer.parseInt(properties.getProperty("phase1.tps.increment", "100000"));

        // Phase 2
        this.phase2Sleep = Integer.parseInt(properties.getProperty("phase2.sleep", "300"));
        this.phase2TpsStart = Integer.parseInt(properties.getProperty("phase2.tps.start", "1000000"));

        // Phase 3
        this.phase3Sleep = Integer.parseInt(properties.getProperty("phase3.sleep", "60"));
        this.phase3TpsStart = Integer.parseInt(properties.getProperty("phase3.tps.start", "1000000"));
        this.phase3TpsIncrement = Integer.parseInt(properties.getProperty("phase3.tps.increment", "100000"));
    }
}
