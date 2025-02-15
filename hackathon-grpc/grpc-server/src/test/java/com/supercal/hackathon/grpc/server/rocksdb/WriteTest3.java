package com.supercal.hackathon.grpc.server.rocksdb;

import com.supercal.hackathon.grpc.proto.Action;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_DUPLICATE_TRANSACTION;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_INSUFFICIENT_FUNDS;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_SUCCESS;

public class WriteTest3 {

    private final RocksDB db;
    private final ColumnFamilyHandle accountsHandle;
    private final ColumnFamilyHandle txnHandle;

    public WriteTest3() throws RocksDBException {
        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, new ColumnFamilyOptions()));
        cfDescriptors.add(new ColumnFamilyDescriptor("accounts".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));
        cfDescriptors.add(new ColumnFamilyDescriptor("txns".getBytes(StandardCharsets.UTF_8), new ColumnFamilyOptions()));
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        DBOptions options = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true);

        this.db = RocksDB.open(options, "db4", cfDescriptors, cfHandles);
        this.accountsHandle = cfHandles.get(1);
        this.txnHandle = cfHandles.get(2);
    }

    public BalanceUpdateResponseBatch changeBalance(BalanceUpdateRequestBatch batch) throws Exception {
        // Preprocess to collect transaction and balance keys
        Set<Long> batchTxnIds = new HashSet<>();
        List<String> txnKeysForDb = new ArrayList<>();
        List<Boolean> isDuplicateInBatch = new ArrayList<>();
        Set<String> balanceKeysSet = new HashSet<>();

        for (BalanceUpdateRequest request : batch.getRequestList()) {
            long txnId = request.getTransactionId();
            if (batchTxnIds.contains(txnId)) {
                isDuplicateInBatch.add(true);
            } else {
                batchTxnIds.add(txnId);
                isDuplicateInBatch.add(false);
                txnKeysForDb.add("txn_" + txnId);
            }
            balanceKeysSet.add("balance_" + request.getAccountId());
        }
        List<String> balanceKeysForDb = new ArrayList<>(balanceKeysSet);

        // Convert keys to bytes using UTF-8
        List<byte[]> txnKeyBytes = new ArrayList<>(txnKeysForDb.size());
        for (String key : txnKeysForDb) {
            txnKeyBytes.add(key.getBytes(StandardCharsets.UTF_8));
        }
        List<byte[]> balanceKeyBytes = new ArrayList<>(balanceKeysForDb.size());
        for (String key : balanceKeysForDb) {
            balanceKeyBytes.add(key.getBytes(StandardCharsets.UTF_8));
        }

        // Batch read for transactions and balances
        List<byte[]> txnValues = db.multiGetAsList(txnKeyBytes);
        List<byte[]> balanceValues = db.multiGetAsList(balanceKeyBytes);

        // Build transaction existence map
        Map<String, Boolean> txnExists = new HashMap<>();
        for (int i = 0; i < txnKeysForDb.size(); i++) {
            txnExists.put(txnKeysForDb.get(i), txnValues.get(i) != null);
        }

        // Build initial balances map
        Map<String, Integer> initialBalances = new HashMap<>();
        for (int i = 0; i < balanceKeysForDb.size(); i++) {
            String key = balanceKeysForDb.get(i);
            byte[] value = balanceValues.get(i);
            initialBalances.put(key, value != null ? Integer.parseInt(new String(value, StandardCharsets.UTF_8)) : 0);
        }

        // Process requests
        WriteBatch writeBatch = new WriteBatch();
        List<BalanceUpdateResponse> responses = new ArrayList<>();
        Map<String, Integer> updatedBalances = new HashMap<>();

        int requestIndex = 0;
        for (BalanceUpdateRequest request : batch.getRequestList()) {
            long txnId = request.getTransactionId();
            String txnKey = "txn_" + txnId;
            String balanceKey = "balance_" + request.getAccountId();

            if (isDuplicateInBatch.get(requestIndex++)) {
                responses.add(AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_DUPLICATE_TRANSACTION, null));
                continue;
            }

            if (txnExists.getOrDefault(txnKey, false)) {
                responses.add(AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_DUPLICATE_TRANSACTION, null));
                continue;
            }

            int currentBalance = initialBalances.getOrDefault(balanceKey, 0);
            currentBalance = updatedBalances.getOrDefault(balanceKey, currentBalance);

            int amount = request.getAmount();
            Action action = request.getAction();

            if (action == Action.ACTION_DEBIT && currentBalance < amount) {
                responses.add(AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_INSUFFICIENT_FUNDS, currentBalance));
                continue;
            }

            int newBalance = calculateNewBalance(currentBalance, amount, action);
            updatedBalances.put(balanceKey, newBalance);

            writeBatch.put(txnKey.getBytes(StandardCharsets.UTF_8), "processed".getBytes(StandardCharsets.UTF_8));
            responses.add(AbstractWriteTest.toResponse(request.getTransactionId(), OPERATION_STATUS_SUCCESS, newBalance));
        }

        // Add final balances to write batch
        updatedBalances.forEach((key, balance) ->
                {
                    try {
                        writeBatch.put(key.getBytes(StandardCharsets.UTF_8),
                                String.valueOf(balance).getBytes(StandardCharsets.UTF_8));
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                }
        );

        // Execute batch write
        try {
            db.write(new WriteOptions(), writeBatch);
        } finally {
            writeBatch.close();
        }

        BalanceUpdateResponseBatch.Builder builder = BalanceUpdateResponseBatch.newBuilder().addAllResponse(responses);

        return builder.build();
    }

    public void generateWithBalance(int numAccounts, int balance) {
        try (WriteBatch writeBatch = new WriteBatch();
             WriteOptions writeOptions = new WriteOptions()) {

            for (int i = 0; i < numAccounts; i++) {
                byte[] key = intToBytes(i);
                byte[] value = intToBytes(balance);

                writeBatch.put(accountsHandle, key, value);
            }

            db.write(writeOptions, writeBatch);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    private int calculateNewBalance(int current, int amount, Action action) {
        return action == Action.ACTION_CREDIT ? current + amount : current - amount;
    }

    private byte[] intToBytes(int x) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
        buffer.putInt(x);
        return buffer.array();
    }

    private byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(x);
        return buffer.array();
    }

    private int bytesToInt(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        return buffer.getInt();
    }
}
