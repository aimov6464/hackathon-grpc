package com.supercal.hackathon.grpc.client.account;

import com.supercal.hackathon.grpc.client.grpc.GrpcConnection;
import com.supercal.hackathon.grpc.client.grpc.GrpcConnectionStateListener;
import com.supercal.hackathon.grpc.proto.AccountServiceGrpc;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import io.github.bucket4j.Bucket;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.Semaphore;

@Slf4j
public class BatchProcessor implements GrpcConnectionStateListener {

    private final GrpcConnection connection;
    private final AccountClientCallback callback;
    private final Semaphore semaphore;
    private final Bucket tpsBucket;
    private final String compression;

    private Thread thread;
    private boolean shutDown = false;

    private StreamObserver<BalanceUpdateRequestBatch> streamObserver;
    private BalanceUpdateRequestBatch inFlightBatch;

    public BatchProcessor(GrpcConnection connection, String compression, AccountClientCallback callback, Bucket tpsBucket) {
        this.connection = connection;
        this.callback = callback;
        this.tpsBucket = tpsBucket;
        this.compression = compression;
        this.streamObserver = toStreamObserver(connection.getChannel(), compression);
        this.semaphore = new Semaphore(1);
        this.connection.addStateChangeListener(this);
    }

    @Override
    public void onReconnect() {
        streamObserver = toStreamObserver(connection.getChannel(), compression);
    }

    public void addBatch(BalanceUpdateRequestBatch batch) {
        try {
            connection.whenReady();
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        if(!shutDown) {
            thread = Thread.startVirtualThread(() -> send(batch));
        }
    }

    public void shutdown() {
        shutDown = true;

        try {
            if(thread != null) {
                thread.join();
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }

        streamObserver.onCompleted();
    }

    private void retryBatch() {
        BalanceUpdateRequestBatch batch = inFlightBatch;
        inFlightBatch = null;

        try {
            connection.whenReady();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        log.debug("Retrying batch :: partition={}, batchId={}", batch.getPartition(), batch.getBatchId());
        callback.retry(batch);
        thread = Thread.startVirtualThread(() -> send(batch));
    }

    private void send(BalanceUpdateRequestBatch batch) {
        try {
            tpsBucket.asBlocking().consume(batch.getRequestCount());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        // Send batch
        inFlightBatch = batch;
        streamObserver.onNext(batch);
        log.debug("Batch sent :: partition={}, batchId={}", batch.getPartition(), batch.getBatchId());

        // Notify callback
        callback.sent(batch);
    }

    private StreamObserver<BalanceUpdateRequestBatch> toStreamObserver(ManagedChannel channel, String compression) {
        AccountServiceGrpc.AccountServiceStub stub = AccountServiceGrpc.newStub(channel).withWaitForReady();

        // Set compression
        if(compression != null) {
            stub = stub.withCompression(compression);
        }

        return stub.balanceUpdate(new StreamObserver<>() {
            @Override
            public void onNext(BalanceUpdateResponseBatch batch) {
                if(batch.getBatchCompleted()) {
                    log.debug("Batch completed :: batchId={}", batch.getBatchId());
                    inFlightBatch = null;
                    semaphore.release();
                } else if (batch.getBatchError()) {
                    retryBatch();
                } else {
                    callback.finished(batch);
                }
            }

            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    Status status = ((StatusRuntimeException) t).getStatus();

                    if (status.getCode() == Status.Code.OK) {
                        return;
                    }

                    log.error("[gRPC] Server error :: status={}, message={}", status.getCode(), t.getMessage());
                    synchronized (semaphore) {
                        if(inFlightBatch != null) {
                            retryBatch();
                        }
                    }
                } else {
                    log.error("onError", t);
                }
            }

            @Override
            public void onCompleted() {
                log.error("should not be called");
            }
        });
    }
}
