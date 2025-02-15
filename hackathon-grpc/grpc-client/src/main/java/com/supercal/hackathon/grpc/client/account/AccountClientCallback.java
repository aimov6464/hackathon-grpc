package com.supercal.hackathon.grpc.client.account;

import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import com.supercal.hackathon.grpc.proto.OperationStatus;
import lombok.Data;
import lombok.Getter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class AccountClientCallback {

    @Getter
    private final Stats stats;
    private final ClientAccountManager clientAccountManager;
    private final Map<Long, BalanceUpdateRequest> sentMap;

    public AccountClientCallback(ClientAccountManager clientAccountManager) {
        this.clientAccountManager = clientAccountManager;
        this.sentMap = new ConcurrentHashMap<>();
        this.stats = new Stats();
    }

    public void sent(BalanceUpdateRequestBatch batch) {
        stats.getSent().addAndGet(batch.getRequestCount());
        batch.getRequestList().forEach(r -> sentMap.put(r.getTransactionId(), r));
    }

    public void retry(BalanceUpdateRequestBatch batch) {
        stats.getRetry().addAndGet(batch.getRequestCount());
    }

    public void finished(BalanceUpdateResponseBatch batch) {
        stats.getFinished().addAndGet(batch.getResponseCount());
        batch.getResponseList().forEach(response -> {
            // Update balance of account in clientAccountManager
            clientAccountManager.changeBalance(sentMap.get(response.getTransactionId()), response);

            // Remove request from sentMap since it has been processed
            sentMap.remove(response.getTransactionId());

            // Duplicate transaction
            if (response.getStatus() == OperationStatus.OPERATION_STATUS_DUPLICATE_TRANSACTION) {
                stats.getDuplicate().incrementAndGet();
            }
        });
    }

    @Data
    public static class Stats {
        private final AtomicLong sent = new AtomicLong(0);
        private final AtomicLong finished = new AtomicLong(0);
        private final AtomicLong retry = new AtomicLong(0);
        private final AtomicLong failed = new AtomicLong(0);
        private final AtomicLong duplicate = new AtomicLong(0);
    }
}
