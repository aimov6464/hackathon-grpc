package com.supercal.hackathon.grpc.server.account.inmemory;

import com.supercal.hackathon.grpc.proto.Action;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import com.supercal.hackathon.grpc.proto.OperationStatus;
import com.supercal.hackathon.grpc.server.GrpcServerConfig;
import com.supercal.hackathon.grpc.server.account.AccountCache;
import com.supercal.hackathon.grpc.server.account.AccountManager;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import lombok.Data;

import java.util.List;

@Data
public class InMemoryAccountManager implements AccountManager {

    private AccountCache accounts;

    public InMemoryAccountManager(GrpcServerConfig grpcServerConfig) {
        this.accounts = new AccountCache(grpcServerConfig.getAccounts());
    }

    @Override
    public void changeBalance(BalanceUpdateRequestBatch batch, StreamObserver<BalanceUpdateResponseBatch> observer) {
        List<BalanceUpdateResponse> responses = batch.getRequestList().stream().map(this::changeBalance).toList();
        BalanceUpdateResponseBatch.Builder builder = BalanceUpdateResponseBatch.newBuilder().addAllResponse(responses);

        observer.onNext(builder.build());
        observer.onNext(BalanceUpdateResponseBatch.newBuilder().setBatchCompleted(true).setBatchId(batch.getBatchId()).build());
    }

    protected BalanceUpdateResponse changeBalance(BalanceUpdateRequest request) {
        int balance = accounts.get(request.getAccountId());

        if (balance == AccountCache.defaultReturnValue) {
            return BalanceUpdateResponse.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setStatus(OperationStatus.OPERATION_STATUS_ACCOUNT_NOT_FOUND)
                    .build();
        }

        if (request.getAmount() <= 0) {
            return BalanceUpdateResponse.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setBalance(balance)
                    .setStatus(OperationStatus.OPERATION_STATUS_INVALID_AMOUNT)
                    .build();
        }

        if (balance < request.getAmount()) {
            return BalanceUpdateResponse.newBuilder()
                    .setTransactionId(request.getTransactionId())
                    .setBalance(balance)
                    .setStatus(OperationStatus.OPERATION_STATUS_INSUFFICIENT_FUNDS)
                    .build();
        }

        int newBalance = request.getAction() == Action.ACTION_DEBIT ? -request.getAmount() : request.getAmount();
        accounts.put(request.getAccountId(), newBalance);

        return BalanceUpdateResponse.newBuilder()
                .setTransactionId(request.getTransactionId())
                .setBalance(newBalance)
                .setStatus(OperationStatus.OPERATION_STATUS_SUCCESS)
                .build();
    }

    public void createAccounts(int accountNumber, int balance) {
        for (int i = 0; i < accountNumber; i++) {
            accounts.put(i, balance);
        }
    }

    @Override
    public Int2IntOpenHashMap getAccounts() {
        return accounts.getMap();
    }
}
