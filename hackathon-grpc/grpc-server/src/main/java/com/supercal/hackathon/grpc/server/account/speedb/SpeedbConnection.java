package com.supercal.hackathon.grpc.server.account.speedb;

import com.supercal.hackathon.grpc.proto.Action;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import com.supercal.hackathon.grpc.server.GrpcServerConfig;
import com.supercal.hackathon.grpc.server.account.AccountCache;
import com.supercal.hackathon.grpc.server.account.AccountManagerUtil;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WALRecoveryMode;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_ACCOUNT_NOT_FOUND;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_DUPLICATE_TRANSACTION;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_FAILED;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_INSUFFICIENT_FUNDS;
import static com.supercal.hackathon.grpc.proto.OperationStatus.OPERATION_STATUS_SUCCESS;
import static com.supercal.hackathon.grpc.server.account.AccountManagerUtil.bytesToInt;
import static com.supercal.hackathon.grpc.server.account.AccountManagerUtil.intToBytes;
import static com.supercal.hackathon.grpc.server.account.AccountManagerUtil.longToBytes;
import static com.supercal.hackathon.grpc.server.account.AccountManagerUtil.toResponse;

@Slf4j
public class SpeedbConnection {

    private final GrpcServerConfig config;
    private final RocksDB db;
    private final ColumnFamilyHandle accountsHandle;
    private final ColumnFamilyHandle txnHandle;
    private final AccountCache cache;
    private final int shard;
    private boolean dbExists;

    public SpeedbConnection(GrpcServerConfig config, int shard) throws RocksDBException {
        this.config = config;
        this.cache = new AccountCache(config.getAccounts());
        this.shard = shard;
        this.dbExists = false;

        ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
                .setWriteBufferSize(512 * 1024 * 1024)
                .setMaxWriteBufferNumber(8)
                .setTargetFileSizeBase(512 * 1024 * 1024)
                .setMaxBytesForLevelBase(1024 * 1024 * 1024)
                .setMinWriteBufferNumberToMerge(2)
                .setCompressionType(CompressionType.NO_COMPRESSION)
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setTableFormatConfig(new BlockBasedTableConfig()
                        .setBlockCacheSize(512 * 1024 * 1024)
                        .setBlockSize(32 * 1024)
                        .setFilterPolicy(new BloomFilter(10, false)))
                .setLevelCompactionDynamicLevelBytes(true);

        List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions));
        cfDescriptors.add(new ColumnFamilyDescriptor("accounts".getBytes(StandardCharsets.UTF_8), cfOptions));
        cfDescriptors.add(new ColumnFamilyDescriptor("txns".getBytes(StandardCharsets.UTF_8), cfOptions));
        List<ColumnFamilyHandle> cfHandles = new ArrayList<>();

        DBOptions dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setCreateMissingColumnFamilies(true)
                .setMaxBackgroundJobs(16)
                .setIncreaseParallelism(4)
                .setMaxBackgroundCompactions(4)
                .setAllowConcurrentMemtableWrite(true)
                .setMaxBackgroundFlushes(4)
                .setUseDirectIoForFlushAndCompaction(true)
                .setUseDirectReads(true)
                .setUseAdaptiveMutex(true)
                .setUnorderedWrite(true)
                .setWalRecoveryMode(WALRecoveryMode.SkipAnyCorruptedRecords)
                .setSkipStatsUpdateOnDbOpen(true)
                .setSkipCheckingSstFileSizesOnDbOpen(true)
                .setMaxOpenFiles(-1);

        // Check if database directory exists and contains RocksDB files
        String dbPath = "db" + shard;
        File dbDir = new File(dbPath);
        this.dbExists = dbDir.exists() && dbDir.isDirectory();

        this.db = RocksDB.open(dbOptions, dbPath, cfDescriptors, cfHandles);
        this.accountsHandle = cfHandles.get(1);
        this.txnHandle = cfHandles.get(2);
    }

    public void changeBalance(StreamObserver<BalanceUpdateResponseBatch> observer, List<BalanceUpdateRequest> requests) {
        BalanceUpdateResponseBatch.Builder builder = BalanceUpdateResponseBatch.newBuilder();

        try (WriteBatch writeBatch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions()) {
            // Set WAL
            if(!config.isWal()) {
                writeOptions.disableWAL();
            }

            // Set to track transaction IDs that we plan to record in this batch.
            Set<Long> txnIdsToAdd = new HashSet<>();

            // Collect transaction Ids and accountIds
            List<Long> txnIds = new ArrayList<>(requests.size());
            List<byte[]> txnKeys = new ArrayList<>(requests.size());
            List<Integer> accountIds = new ArrayList<>();
            requests.forEach(r -> {
                txnIds.add(r.getTransactionId());
                txnKeys.add(longToBytes(r.getTransactionId()));
                accountIds.add(r.getAccountId());
            });

            // Get duplicate transactions
            Map<Long, byte[]> duplicateTransactions = getDuplicateTransactions(txnIds, txnKeys);

            // Get accounts
            Map<Integer, Integer> accounts = getAccounts(accountIds);

            // Process each request in order.
            for (BalanceUpdateRequest request : requests) {
                try  {
                    // Check persistent storage or existing batch for a duplicate transaction.
                    if (txnIdsToAdd.contains(request.getTransactionId()) || duplicateTransactions.containsKey(request.getTransactionId())) {
                        BalanceUpdateResponse response = toResponse(request, OPERATION_STATUS_DUPLICATE_TRANSACTION);
                        builder.addResponse(response);
                        continue;
                    }

                    // Get current balance for the account.
                    Integer accountBalance = accounts.get(request.getAccountId());
                    if (accountBalance == null) {
                        BalanceUpdateResponse response = toResponse(request, OPERATION_STATUS_ACCOUNT_NOT_FOUND);
                        builder.addResponse(response);
                        continue;
                    }

                    // Process deposit or withdrawal.
                    if (request.getAction() == Action.ACTION_CREDIT) {
                        accounts.put(request.getAccountId(), accountBalance + request.getAmount());
                    } else if (request.getAction() == Action.ACTION_DEBIT) {
                        if (accountBalance < request.getAmount()) {
                            BalanceUpdateResponse response = toResponse(request, OPERATION_STATUS_INSUFFICIENT_FUNDS, accountBalance);
                            builder.addResponse(response);
                            continue;
                        }
                        accounts.put(request.getAccountId(), accountBalance - request.getAmount());
                    } else {
                        BalanceUpdateResponse response = toResponse(request, OPERATION_STATUS_FAILED, accountBalance);
                        builder.addResponse(response);
                        continue;
                    }

                    // Update the in-memory balance.
                    BalanceUpdateResponse response = toResponse(request, OPERATION_STATUS_SUCCESS, accounts.get(request.getAccountId()));
                    builder.addResponse(response);

                    // Mark this transaction ID to be recorded.
                    txnIdsToAdd.add(request.getTransactionId());
                } catch (Exception e) {
                    log.error("Error processing request", e);
                    BalanceUpdateResponse response = toResponse(request, OPERATION_STATUS_FAILED);
                    builder.addResponse(response);
                }
            }

            // Add all account balance updates to the WriteBatch.
            for (Map.Entry<Integer, Integer> entry : accounts.entrySet()) {
                byte[] accountKey = intToBytes(entry.getKey());
                byte[] balanceBytes = intToBytes(entry.getValue());
                writeBatch.put(accountsHandle, accountKey, balanceBytes);
            }

            // Add all processed transaction IDs to the WriteBatch.
            for (long txnId : txnIdsToAdd) {
                byte[] transactionIdKey = longToBytes(txnId);
                byte[] transactionIdValue = new byte[]{1};
                writeBatch.put(txnHandle, transactionIdKey, transactionIdValue);
            }

            // Write the entire batch atomically.
            db.write(writeOptions, writeBatch);
            cache.putAll(accounts);
        } catch (RocksDBException e) {
            log.error("Error processing batch", e);
        }

        observer.onNext(builder.build());
    }

    public Int2IntOpenHashMap getAccounts() {
        Int2IntOpenHashMap accountMap = new Int2IntOpenHashMap();

        try (RocksIterator iterator = db.newIterator(accountsHandle)) {
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                int accountId = bytesToInt(iterator.key());
                int balance = bytesToInt(iterator.value());

                accountMap.put(accountId, balance);
            }
        }

        log.info("getAccounts :: shard={}, count={}", shard, accountMap.size());
        return accountMap;
    }

    public void createAccounts(List<Integer> accounts, int balance) throws RocksDBException {
        log.info("createAccounts :: shard={}, count={}", shard, accounts.size());

        if(dbExists) {
            log.info("database already exists :: shard={}", shard);
            return;
        }

        try (WriteBatch writeBatch = new WriteBatch(); WriteOptions writeOptions = new WriteOptions().setSync(false)) {
            for (int accountId : accounts) {
                byte[] key = intToBytes(accountId);
                byte[] value = intToBytes(balance);
                writeBatch.put(accountsHandle, key, value);
            }

            db.write(writeOptions, writeBatch);
        }
    }

    public void shutdown() {
        db.close();
    }

    private Map<Integer, Integer> getAccounts(List<Integer> accountIds) throws RocksDBException {
        Map<Integer, Integer> accounts = new Int2IntOpenHashMap(accountIds.size());
        List<Integer> notCachedAccounts = new ArrayList<>();

        // Fetch accounts
        for (int accountId : accountIds) {
            int balance = cache.get(accountId);

            if (balance != AccountCache.defaultReturnValue) {
                accounts.put(accountId, balance);
            } else {
                notCachedAccounts.add(accountId);
            }
        }

        // Create account keys
        List<byte[]> accountKeys = notCachedAccounts.stream()
                .map(AccountManagerUtil::intToBytes)
                .collect(Collectors.toList());

        // Get accounts from db
        List<ColumnFamilyHandle> accountHandles = Collections.nCopies(accountKeys.size(), accountsHandle);
        List<byte[]> accountValues = db.multiGetAsList(accountHandles, accountKeys);

        for (int i = 0; i < notCachedAccounts.size(); i++) {
            int accountId = notCachedAccounts.get(i);
            byte[] value = accountValues.get(i);
            accounts.put(accountId, (value != null) ? bytesToInt(value) : 0);
        }

        return accounts;
    }

    private Map<Long, byte[]> getDuplicateTransactions(List<Long> txnIds, List<byte[]> txnKeys) throws RocksDBException {
        // Collect duplicate transactions
        Long2ObjectOpenHashMap<byte[]> duplicateTransactions = new Long2ObjectOpenHashMap<>(txnIds.size());

        // Batch read for existing transactions
        List<ColumnFamilyHandle> txnHandles = Collections.nCopies(txnKeys.size(), txnHandle);
        List<byte[]> existingValues = db.multiGetAsList(txnHandles, txnKeys);

        for (int i = 0; i < txnIds.size(); i++) {
            if (existingValues.get(i) != null) {
                duplicateTransactions.put(txnIds.get(i), existingValues.get(i));
            }
        }

        return duplicateTransactions;
    }
}
