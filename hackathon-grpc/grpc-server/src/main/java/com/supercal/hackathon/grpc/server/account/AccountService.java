package com.supercal.hackathon.grpc.server.account;

import com.supercal.hackathon.grpc.proto.AccountServiceGrpc;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AccountService extends AccountServiceGrpc.AccountServiceImplBase {

    private final AccountManager accountManager;
    private final Map<Integer, PartitionProcessor> partitionProcessors;

    public AccountService(AccountManager accountManager) {
        this.accountManager = accountManager;
        this.partitionProcessors = new ConcurrentHashMap<>();
    }

    @Override
    public StreamObserver<BalanceUpdateRequestBatch> balanceUpdate(StreamObserver<BalanceUpdateResponseBatch> responseObserver) {
        return new StreamObserver<>() {

            private int partition;
            private PartitionProcessor partitionProcessor;

            @Override
            public void onNext(BalanceUpdateRequestBatch batch) {
                if (partitionProcessor == null) {
                    this.partitionProcessor = partitionProcessors.computeIfAbsent(batch.getPartition(), p -> new PartitionProcessor());
                    this.partition = batch.getPartition();
                }

                partitionProcessor.addBatch(batch, responseObserver);
            }

            @Override
            public void onError(Throwable t) {

            }

            @Override
            public void onCompleted() {
                partitionProcessor.shutdown();
                responseObserver.onCompleted();
                partitionProcessors.remove(partition);
            }
        };
    }

    public class PartitionProcessor {
        private final ThreadPoolExecutor executor;

        public PartitionProcessor() {
            this.executor = new ThreadPoolExecutor(
                    1,
                    1,
                    0,
                    TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(),
                    Thread.ofVirtual().factory());
        }

        public void addBatch(BalanceUpdateRequestBatch batch, StreamObserver<BalanceUpdateResponseBatch> observer) {
            executor.execute(() -> {
                try {
                    accountManager.changeBalance(batch, observer);
                } catch (Exception e) {
                    log.error("Exception adding batch", e);
                    observer.onError(e);
                }
            });
        }

        public void shutdown() {
            executor.shutdown();

            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException ex) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
