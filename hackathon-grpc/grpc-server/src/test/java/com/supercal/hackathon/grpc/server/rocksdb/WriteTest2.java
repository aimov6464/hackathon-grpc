package com.supercal.hackathon.grpc.server.rocksdb;

import com.supercal.hackathon.grpc.proto.Action;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_DUPLICATE_TRANSACTION;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_INSUFFICIENT_FUNDS;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_SUCCESS;
import static com.supercal.hackathon.grpc.server.rocksdb.AbstractWriteTest.toResponse;

public class WriteTest2 {

    private final RocksDB db;

    public WriteTest2() throws RocksDBException {
        Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        this.db = RocksDB.open(options, "db2");
    }

    public BalanceUpdateResponseBatch changeBalance(BalanceUpdateRequestBatch batch) throws Exception {
        // 1) Collect all keys to read in a single pass
        List<byte[]> txnKeys = new ArrayList<>();
        List<byte[]> balanceKeys = new ArrayList<>();
        for (BalanceUpdateRequest request : batch.getRequestList()) {
            txnKeys.add(("txn_" + request.getTransactionId()).getBytes());
            balanceKeys.add(("balance_" + request.getAccountId()).getBytes());
        }

        // 2) multiGet
        List<byte[]> txnValues = db.multiGetAsList(txnKeys);
        List<byte[]> balanceValues = db.multiGetAsList(balanceKeys);

        Map<byte[], byte[]> txnKeyValueMap = new HashMap<>();
        for (int i = 0; i < txnKeys.size(); i++) {
            byte[] k = txnKeys.get(i);
            byte[] v = txnValues.get(i);
            txnKeyValueMap.put(k, v);
        }

        Map<byte[], byte[]> balanceKeyValueMap = new HashMap<>();
        for (int i = 0; i < balanceKeys.size(); i++) {
            byte[] k = balanceKeys.get(i);
            byte[] v = balanceValues.get(i);
            balanceKeyValueMap.put(k, v);
        }

        WriteBatch writeBatch = new WriteBatch();
        List<BalanceUpdateResponse> responses = new ArrayList<>();

        // 3) Update in memory, build one big batch
        for (BalanceUpdateRequest request : batch.getRequestList()) {
            // Construct keys again or keep them in sync with the array lists
            byte[] transactionKey = ("txn_" + request.getTransactionId()).getBytes();
            byte[] balanceKey = ("balance_" + request.getAccountId()).getBytes();

            if (txnKeyValueMap.get(transactionKey) != null) {
                // Duplicate transaction
                responses.add(toResponse(request.getTransactionId(), OPERATION_STATUS_DUPLICATE_TRANSACTION, null));
                continue;
            }

            byte[] currentBalanceBytes = balanceKeyValueMap.get(balanceKey);
            int currentBalance = currentBalanceBytes == null ? 0 : Integer.parseInt(new String(currentBalanceBytes));
            int amount = request.getAmount();

            if (request.getAction() == Action.ACTION_DEBIT && currentBalance < amount) {
                responses.add(toResponse(request.getTransactionId(), OPERATION_STATUS_INSUFFICIENT_FUNDS, currentBalance));
                continue;
            }

            int newBalance = request.getAction() == Action.ACTION_CREDIT
                    ? currentBalance + amount
                    : currentBalance - amount;

            writeBatch.put(balanceKey, String.valueOf(newBalance).getBytes());
            writeBatch.put(transactionKey, "processed".getBytes());
            responses.add(toResponse(request.getTransactionId(), OPERATION_STATUS_SUCCESS, newBalance));
        }

        // 4) Do one single write
        db.write(new WriteOptions(), writeBatch);

        BalanceUpdateResponseBatch.Builder builder = BalanceUpdateResponseBatch.newBuilder().addAllResponse(responses);

        return builder.build();
    }
}
