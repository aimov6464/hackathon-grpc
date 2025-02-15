package com.supercal.hackathon.grpc.server.account.redis;

import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import com.supercal.hackathon.grpc.proto.OperationStatus;
import com.supercal.hackathon.grpc.server.GrpcServerConfig;
import com.supercal.hackathon.grpc.server.account.AccountManager;
import io.grpc.stub.StreamObserver;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

@Slf4j
public class RedisAccountManager implements AccountManager {

    private static final String ACCOUNT_PREFIX = "acc:";
    private final JedisPool jedisPool;

    private final static String ADD_BALANCE = """
            local balance = tonumber(redis.call("HGET", KEYS[1], "a"))
               local increment = tonumber(ARGV[1])
            
               if balance then
                   local newBalance = balance + increment
                   if newBalance >= 0 then
                       redis.call("HSET", KEYS[1], "a", newBalance)
                       return newBalance
                   else
                       return "INSUFFICIENT_FUNDS"  -- More explicit error
               else
                   return "ACCOUNT_NOT_FOUND"  -- More explicit error
               end
            """;

    public RedisAccountManager(GrpcServerConfig grpcServerConfig) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(grpcServerConfig.getRedisConnections());
        config.setMaxIdle(grpcServerConfig.getRedisConnections());
        config.setMinIdle(grpcServerConfig.getRedisConnections());

        this.jedisPool = new JedisPool(config, grpcServerConfig.getRedisHost(), grpcServerConfig.getRedisPort());
    }

    @Override
    public void createAccounts(int numAccounts, int initialAmount) {
        try (Jedis jedis = jedisPool.getResource()) {
            for (int uid = 1; uid <= numAccounts; uid++) {
                String accountKey = ACCOUNT_PREFIX + uid;
                jedis.hset(accountKey, "a", String.valueOf(initialAmount)); // Set each account amount
            }

            System.out.println("Generated " + numAccounts + " accounts with initial amount: " + initialAmount);
        }
    }

//    @Override
//    public long getNumberOfAccounts() {
//        try (Jedis jedis = jedisPool.getResource()) {
//            ScanParams scanParams = new ScanParams().match(ACCOUNT_PREFIX + "*").count(1000);  // Scan in chunks of 1000
//            String cursor = "0";
//            long accountCount = 0;
//
//            do {
//                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
//                cursor = scanResult.getCursor();
//                accountCount += scanResult.getResult().size();
//            } while (!cursor.equals("0"));
//
//            return accountCount;
//        } catch (Exception e) {
//            log.error("Error getting number of accounts: {}", e.getMessage());
//            return 0;
//        }
//    }

//    @Override
//    public void addToAllAccounts(int amount) {
//        try (Jedis jedis = jedisPool.getResource()) {
//            ScanParams scanParams = new ScanParams().match(ACCOUNT_PREFIX + "*").count(1000);
//            String cursor = "0";
//
//            do {
//                ScanResult<String> scanResult = jedis.scan(cursor, scanParams);
//                cursor = scanResult.getCursor();
//
//                for (String key : scanResult.getResult()) {
//                    long currentAmount = Long.parseLong(jedis.hget(key, "a"));
//                    jedis.hset(key, "a", String.valueOf(currentAmount + amount));
//                }
//            } while (!cursor.equals("0"));
//
//            log.info("Added {} to all accounts.", amount);
//        } catch (Exception e) {
//            log.error("Error adding to all accounts: {}", e.getMessage());
//        }
//    }

    @Override
    public Int2IntOpenHashMap getAccounts() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void changeBalance(BalanceUpdateRequestBatch batch, StreamObserver<BalanceUpdateResponseBatch> observer) {
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();

            for (BalanceUpdateRequest request : batch.getRequestList()) {
                p.eval(ADD_BALANCE, request.getAccountId(), String.valueOf(request.getAmount()));
            }
            p.sync();

        } catch (Exception e) {
            log.error("Unexpected error changing balance for account: {}", e.getMessage());
        }
    }

    public BalanceUpdateResponse changeBalance(BalanceUpdateRequest request) {
        int accountId = request.getAccountId();
        String accountKey = ACCOUNT_PREFIX + accountId;

        try (Jedis jedis = jedisPool.getResource()) {
            // Execute the Lua script
            String result = (String) jedis.eval(ADD_BALANCE, 1, accountKey, String.valueOf(request.getAmount()));

            // Handle Lua script results
            if ("INSUFFICIENT_FUNDS".equals(result)) {
                log.warn("Insufficient funds for account: {}", accountId);
                return BalanceUpdateResponse.newBuilder().setStatus(OperationStatus.OPERATION_STATUS_INSUFFICIENT_FUNDS).build();
            } else if ("ACCOUNT_NOT_FOUND".equals(result)) {
                log.warn("Account not found: {}", accountId);
                return BalanceUpdateResponse.newBuilder().setStatus(OperationStatus.OPERATION_STATUS_ACCOUNT_NOT_FOUND).build();
            }

            // Successfully changed balance
            log.info("Balance for account {} changed successfully. New balance: {}", accountId, result);
        } catch (JedisException e) {
            log.error("Jedis error changing balance for account {}: {}", accountId, e.getMessage());
            return BalanceUpdateResponse.newBuilder().setStatus(OperationStatus.OPERATION_STATUS_FAILED).build();
        } catch (Exception e) {
            log.error("Unexpected error changing balance for account {}: {}", accountId, e.getMessage());
            return BalanceUpdateResponse.newBuilder().setStatus(OperationStatus.OPERATION_STATUS_FAILED).build();
        }

        return BalanceUpdateResponse.newBuilder().setStatus(OperationStatus.OPERATION_STATUS_SUCCESS).build();
    }

    public void deleteAllAccounts() {
        try (Jedis jedis = jedisPool.getResource()) {
            String cursor = "0";
            String pattern = ACCOUNT_PREFIX + "*";

            do {
                ScanResult<String> scanResult = jedis.scan(cursor, new ScanParams().match(pattern));
                cursor = scanResult.getCursor();
                if (!scanResult.getResult().isEmpty()) {
                    jedis.del(scanResult.getResult().toArray(new String[0]));
                }
            } while (!cursor.equals("0"));

            log.info("Deleted all accounts.");
        } catch (Exception e) {
            log.error("Error deleting accounts: {}", e.getMessage());
        }
    }
}