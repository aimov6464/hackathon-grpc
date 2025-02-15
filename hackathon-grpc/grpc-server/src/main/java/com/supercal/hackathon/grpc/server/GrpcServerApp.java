package com.supercal.hackathon.grpc.server;

import com.supercal.hackathon.grpc.commons.account.AccountFileUtil;
import com.supercal.hackathon.grpc.commons.properties.PropertiesReader;
import com.supercal.hackathon.grpc.server.account.AccountManager;
import com.supercal.hackathon.grpc.server.account.AccountService;
import com.supercal.hackathon.grpc.server.account.inmemory.InMemoryAccountManager;
import com.supercal.hackathon.grpc.server.account.redis.RedisAccountManager;
import com.supercal.hackathon.grpc.server.account.speedb.SpeedbAccountManager;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.incubator.channel.uring.IOUring;
import io.netty.incubator.channel.uring.IOUringEventLoopGroup;
import io.netty.incubator.channel.uring.IOUringServerSocketChannel;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.Executors;

@Slf4j
public class GrpcServerApp {

    private final GrpcServerConfig config;
    private AccountManager accountManager;

    public static void main(String... args) throws Exception {
        new GrpcServerApp();
    }

    public GrpcServerApp() throws Exception {
        // Read properties
        Properties properties = PropertiesReader.readProperties("config.properties");
        this.config = new GrpcServerConfig(properties);

        // Init accounts
        init();

        // Start server
        startServer();
    }

    private void init() throws Exception {
        accountManager = switch (config.getAccountManager()) {
            case "memory" -> new InMemoryAccountManager(config);
            case "redis" -> new RedisAccountManager(config);
            case "speedb" -> new SpeedbAccountManager(config);
            default -> throw new RuntimeException("Unsupported account-manager=" + config.getAccountManager());
        };

        accountManager.createAccounts(config.getAccounts(), config.getAccountBalance());
    }

    private void startServer() throws InterruptedException, IOException {
        // Build server
        NettyServerBuilder builder = NettyServerBuilder.forPort(config.getPort())
                .maxInboundMessageSize(config.getMaxInboundMessageSize() * 1024 * 1024)
                .flowControlWindow(config.getFlowControlWindow() * 1024 * 1024)
                .addService(new AccountService(accountManager));

        // Executor
        if(config.isThreadsVirtual()) {
            builder.executor(Executors.newVirtualThreadPerTaskExecutor());
        } else {
            builder.executor(Executors.newFixedThreadPool(config.getThreads()));
        }

        // IO_uring
        if (config.isIoUring()) {
            if(IOUring.isAvailable()) {
                log.info("Using IO_uring");
                builder
                        .bossEventLoopGroup(new IOUringEventLoopGroup())
                        .workerEventLoopGroup(new IOUringEventLoopGroup())
                        .channelType(IOUringServerSocketChannel.class);
            } else if(KQueue.isAvailable()) {
                log.info("Using KQueue");
                builder
                        .bossEventLoopGroup(new KQueueEventLoopGroup())
                        .workerEventLoopGroup(new KQueueEventLoopGroup())
                        .channelType(KQueueServerSocketChannel.class);
            } else {
                log.info("IO_uring not supported");
            }
        }

        // Start server
        log.info("Server started on port={}", config.getPort());
        Server server = builder.build();
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered");

            // Shutdown grpc server
            server.shutdown();

            // Shutdown grpc server
            Map<Integer, Integer> accountMap = accountManager.getAccounts();
            AccountFileUtil.writeToFile(new TreeMap<>(accountMap) ,"account-server.txt");

            // Shutdown account-manager
            accountManager.shutdown();
        }));

        server.awaitTermination();
    }
}
