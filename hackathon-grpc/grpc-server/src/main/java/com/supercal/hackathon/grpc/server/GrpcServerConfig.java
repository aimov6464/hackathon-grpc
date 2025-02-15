package com.supercal.hackathon.grpc.server;

import lombok.Data;

import java.util.Properties;

@Data
public class GrpcServerConfig {

    private final Properties properties;

    // Server
    public final int port;
    public final boolean threadsVirtual;
    public final int threads;
    public final int maxInboundMessageSize;
    public final int flowControlWindow;
    public final boolean ioUring;

    // Account
    public final int accounts;
    public final int accountBalance;
    public final String accountManager;

    // Speedb
    public final int shards;
    public final boolean wal;

    // Redis
    public final String redisHost;
    public final int redisPort;
    public final int redisConnections;

    // Error
    public final int simulateError;

    public GrpcServerConfig(Properties properties) {
        this.properties = properties;

        // Server
        this.port = Integer.parseInt(properties.getProperty("server.port", "50051"));
        this.threadsVirtual = Boolean.parseBoolean(properties.getProperty("server.threads.virtual", "true"));
        this.threads = Integer.parseInt(properties.getProperty("server.threads", "4"));
        this.maxInboundMessageSize = Integer.parseInt(properties.getProperty("server.max.inbound.message.size", "1"));
        this.flowControlWindow = Integer.parseInt(properties.getProperty("server.flow.control.window", "4"));
        this.ioUring = Boolean.parseBoolean(properties.getProperty("server.iouring", "true"));

        // Account
        this.accounts = Integer.parseInt(properties.getProperty("account.count", "100000"));
        this.accountBalance = Integer.parseInt(properties.getProperty("account.balance", "1000"));
        this.accountManager = properties.getProperty("account.manager", "memory");

        // Speedb
        this.shards = Integer.parseInt(properties.getProperty("speedb.shards", "2"));
        this.wal = Boolean.parseBoolean(properties.getProperty("speedb.wal", "true"));

        // Redis
        this.redisHost = properties.getProperty("redis.host", "localhost");
        this.redisPort = Integer.parseInt(properties.getProperty("redis.port", "6379"));
        this.redisConnections = Integer.parseInt(properties.getProperty("redis.connections", "100"));

        // Error
        this.simulateError = Integer.parseInt(properties.getProperty("simulate.error.percent", "0"));
    }
}
