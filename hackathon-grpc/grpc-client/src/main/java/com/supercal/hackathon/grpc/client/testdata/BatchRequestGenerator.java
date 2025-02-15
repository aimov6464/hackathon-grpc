package com.supercal.hackathon.grpc.client.testdata;

import com.supercal.hackathon.grpc.client.GrpcClientConfig;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import io.hypersistence.tsid.TSID;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class BatchRequestGenerator {

    private final GrpcClientConfig config;
    private final AtomicInteger batchCounter;
    private final TSID.Factory tsidFactory;
    private boolean stop = false;

    public BatchRequestGenerator(GrpcClientConfig config) {
        this.config = config;
        this.batchCounter = new AtomicInteger();
        this.tsidFactory = TSID.Factory.builder()
                .withNodeBits(0)
                .build();
    }

    public void start(Consumer<BalanceUpdateRequestBatch> consumer) {
        for (int i = 0; i < config.getPartitionsCount(); i++) {
            int partition = i;

            // Start a new thread to generate batches for this partition
            Thread.startVirtualThread(() -> {
                while(!Thread.interrupted() && !stop) {
                    BalanceUpdateRequestBatch batch = generateBatch(partition);
                    consumer.accept(batch);

                    // Send duplicate batch
                    if (ThreadLocalRandom.current().nextInt(100) < config.getSimulateDuplicate()) {
                        consumer.accept(batch);
                    }
                }
            });
        }
    }

    public void shutdown() {
        stop = true;
    }

    public BalanceUpdateRequestBatch generateBatch(int partition) {
        Random random = ThreadLocalRandom.current();

        int partitionSize = config.getAccounts() / config.getPartitionsCount();
        int start = partition * partitionSize;
        int end = Math.min(start + partitionSize, config.getAccounts());
        int[] accountIds = random.ints(config.getAccounts(), start, end).toArray();
        int[] actions = random.ints(config.getBatchSize(), 0, 2).toArray();

        BalanceUpdateRequestBatch.Builder builder = BalanceUpdateRequestBatch.newBuilder().setPartition(partition);
        for (int i = 0; i < config.getBatchSize(); i++) {
            BalanceUpdateRequest request = BalanceUpdateRequest.newBuilder()
                    .setTransactionId(tsidFactory.generate().toLong())
                    .setAccountId(accountIds[i])
                    .setActionValue(actions[i])
                    .setAmount(config.getAmount())
                    .build();

            builder.addRequest(request);
        }

        return builder.setBatchId(batchCounter.getAndIncrement()).build();
    }
}
