package com.supercal.hackathon.grpc.server.account;

import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.OperationStatus;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AccountManagerUtil {

    private static final ThreadLocal<BalanceUpdateResponse.Builder> responseBuilder =
            ThreadLocal.withInitial(BalanceUpdateResponse::newBuilder);

    public static byte[] intToBytes(int x) {
        return new byte[] {
                (byte) (x >>> 24), (byte) (x >>> 16), (byte) (x >>> 8), (byte) x
        };
    }

    public static byte[] longToBytes(long x) {
        return new byte[] {
                (byte) (x >>> 56), (byte) (x >>> 48), (byte) (x >>> 40), (byte) (x >>> 32),
                (byte) (x >>> 24), (byte) (x >>> 16), (byte) (x >>> 8),  (byte) x
        };
    }

    public static int bytesToInt(byte[] bytes) {
        return (bytes[0] & 0xFF) << 24 |
                (bytes[1] & 0xFF) << 16 |
                (bytes[2] & 0xFF) << 8  |
                (bytes[3] & 0xFF);
    }

    public static BalanceUpdateResponse toResponse(BalanceUpdateRequest request, OperationStatus status, Integer newBalance) {
        BalanceUpdateResponse.Builder builder = responseBuilder.get().clear();
        builder.setTransactionId(request.getTransactionId()).setStatus(status);

        if(newBalance != null) {
            builder.setBalance(newBalance);
        }

        return builder.build();
    }

    public static BalanceUpdateResponse toResponse(BalanceUpdateRequest request, OperationStatus status) {
        return toResponse(request, status, null);
    }
}
