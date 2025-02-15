package com.supercal.hackathon.grpc.client.account;

import com.supercal.hackathon.grpc.client.GrpcClientConfig;
import com.supercal.hackathon.grpc.client.grpc.GrpcChannelManager;
import com.supercal.hackathon.grpc.client.grpc.GrpcConnection;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.TokensInheritanceStrategy;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class AccountClient {

    private final GrpcClientConfig config;
    private final Bucket tpsBucket;
    private final Map<Integer, BatchProcessor> batchProcessors;

    public AccountClient(
            GrpcClientConfig config,
            GrpcChannelManager grpcChannelManager,
            AccountClientCallback callback)
    {
        this.config = config;
        this.batchProcessors = new ConcurrentHashMap<>();

        // TPS rate limiter
        tpsBucket = Bucket.builder()
                .addLimit(limit -> limit.capacity(config.getTps()).refillGreedy(config.getTps(), Duration.ofSeconds(1)))
                .build();

        // Create one batch processor per partition
        for (int i = 0; i < config.getPartitionsCount(); i++) {
            GrpcConnection connection = grpcChannelManager.getConnection();
            BatchProcessor batchProcessor = new BatchProcessor(connection, config.getCompression(), callback, tpsBucket);
            batchProcessors.put(i, batchProcessor);
        }
    }

    public void addBatch(BalanceUpdateRequestBatch batch) {
        batchProcessors.get(batch.getPartition()).addBatch(batch);
    }

    public void setTps(int tps) {
        BucketConfiguration newConfig = BucketConfiguration.builder()
                .addLimit(limit -> limit.capacity(tps).refillGreedy(tps, Duration.ofSeconds(1)))
                .build();

        tpsBucket.replaceConfiguration(newConfig, TokensInheritanceStrategy.AS_IS);
    }

    public void shutdown() {
        batchProcessors.entrySet().parallelStream().forEach(entry -> {
            log.info("Shutting down BatchProcessor :: partition={}", entry.getKey());
            entry.getValue().shutdown();
        });
    }
}
