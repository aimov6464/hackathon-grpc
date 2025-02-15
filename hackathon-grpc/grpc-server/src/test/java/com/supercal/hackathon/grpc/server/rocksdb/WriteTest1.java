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

import java.util.List;

import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_DUPLICATE_TRANSACTION;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_INSUFFICIENT_FUNDS;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_SUCCESS;

public class WriteTest1 {

    private final RocksDB db;

    public WriteTest1() throws RocksDBException {
        Options options = new Options()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        this.db = RocksDB.open(options, "db1");
    }

    public BalanceUpdateResponseBatch changeBalance(BalanceUpdateRequestBatch batch) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        List<BalanceUpdateResponse> responses = batch.getRequestList().stream().map(r -> {
            try {
                return updateBalance(r, db, writeBatch);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }).toList();
        db.write(new org.rocksdb.WriteOptions(), writeBatch);

        BalanceUpdateResponseBatch.Builder builder = BalanceUpdateResponseBatch.newBuilder().addAllResponse(responses);

        return builder.build();
    }

    private BalanceUpdateResponse updateBalance(BalanceUpdateRequest request, RocksDB db, WriteBatch writeBatch) throws RocksDBException {
        String transactionKey = "txn_" + request.getTransactionId();
        String balanceKey = "balance_" + request.getAccountId();

        // Check if the transaction has already been processed
        if (db.get(transactionKey.getBytes()) != null) {
            return AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_DUPLICATE_TRANSACTION, null);
        }

        // Fetch current balance
        byte[] currentBalanceBytes = db.get(balanceKey.getBytes());
        int currentBalance = currentBalanceBytes == null ? 0 : Integer.parseInt(new String(currentBalanceBytes));
        int amount = request.getAmount();

        // Check for insufficient funds
        if (request.getAction() == Action.ACTION_DEBIT && currentBalance < amount) {
            return AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_INSUFFICIENT_FUNDS, currentBalance);
        }

        // Calculate new balance
        int newBalance = request.getAction() == Action.ACTION_CREDIT
                ? currentBalance + amount
                : currentBalance - amount;

        // Update balance and log the transaction
        writeBatch.put(balanceKey.getBytes(), String.valueOf(newBalance).getBytes());
        writeBatch.put(transactionKey.getBytes(), "processed".getBytes());

        return AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_SUCCESS, newBalance);
    }
}
