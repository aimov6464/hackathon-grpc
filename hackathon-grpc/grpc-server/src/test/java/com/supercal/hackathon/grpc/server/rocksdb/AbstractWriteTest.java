package com.supercal.hackathon.grpc.server.rocksdb;

import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.OperationStatus;

public class AbstractWriteTest {

    public static BalanceUpdateResponse toResponse(long transactionId, OperationStatus status, Integer newBalance) {
        BalanceUpdateResponse.Builder builder = BalanceUpdateResponse.newBuilder()
                .setTransactionId(transactionId)
                .setStatus(status);

        if(newBalance != null) {
            builder.setBalance(newBalance);
        }

        return builder.build();
    }
}
