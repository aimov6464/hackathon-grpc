package com.supercal.hackathon.grpc.server.account.speedb;

import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import com.supercal.hackathon.grpc.server.GrpcServerConfig;
import com.supercal.hackathon.grpc.server.account.AccountManager;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class SpeedbAccountManager implements AccountManager {

    private final GrpcServerConfig config;
    private final Map<Integer, SpeedbConnection> dbMap;

    public SpeedbAccountManager(GrpcServerConfig config) {
        this.config = config;
        this.dbMap = new ConcurrentHashMap<>();

        log.info("Opening database...");
        List<CompletableFuture<Void>> futures = IntStream.range(0, config.getShards()).mapToObj(i -> CompletableFuture.runAsync(() -> {
            SpeedbConnection conn;
            try {
                conn = new SpeedbConnection(config, i);
                dbMap.put(i, conn);
                log.info("Opened new Speedb connection :: shard={}", i);
            } catch (RocksDBException e) {
                log.error("Failed to open RocksDB", e);
                throw new RuntimeException(e);
            }
        })).toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        log.info("db size={}", dbMap.size());
    }

    @Override
    public void changeBalance(BalanceUpdateRequestBatch batch, StreamObserver<BalanceUpdateResponseBatch> observer) {
        log.debug("Received batch :: partition={}, batch={}", batch.getPartition(), batch.getBatchId());

        // Simulate errors
        if(ThreadLocalRandom.current().nextInt(100) < config.getSimulateError()) {
            log.debug("Simulated error :: partition={}, batchId={}", batch.getPartition(), batch.getBatchId());

            // Create simulated error
            observer.onNext(BalanceUpdateResponseBatch.newBuilder()
                    .setBatchError(true)
                    .setPartition(batch.getPartition())
                    .setBatchId(batch.getBatchId()).build());

            return;
        }

        // Split requests into shard batches
        Map<Integer, List<BalanceUpdateRequest>> requestMapByShard = batch.getRequestList().stream()
                .collect(Collectors.groupingBy(r -> r.getAccountId() % dbMap.size()));

        // Process shard batches
        requestMapByShard.forEach((key, value) -> dbMap.get(key).changeBalance(observer, value));

        // Return batch completed
        observer.onNext(BalanceUpdateResponseBatch.newBuilder().setBatchCompleted(true).setBatchId(batch.getBatchId()).build());
        log.debug("Completed batch :: partition={}, batch={}", batch.getPartition(), batch.getBatchId());
    }

    @Override
    public Int2IntOpenHashMap getAccounts() {
        Int2IntOpenHashMap accountMap = new Int2IntOpenHashMap();
        dbMap.values().forEach(conn -> accountMap.putAll(conn.getAccounts()));

        return accountMap;
    }

    @Override
    public void createAccounts(int numAccounts, int balance) throws RocksDBException {
        Map<Integer, List<Integer>> accountMap = new HashMap<>(numAccounts);

        // Split accounts for the shards
        for (int i = 0; i < numAccounts; i++) {
            int shard = i % dbMap.size();
            List<Integer> accounts = accountMap.computeIfAbsent(shard, s -> new ArrayList<>());
            accounts.add(i);
        }

        // Create accounts
        for (Map.Entry<Integer, List<Integer>> entry : accountMap.entrySet()) {
            dbMap.get(entry.getKey()).createAccounts(entry.getValue(), balance);
        }
    }

    @Override
    public void shutdown() {
        dbMap.values().forEach(SpeedbConnection::shutdown);
    }
}
