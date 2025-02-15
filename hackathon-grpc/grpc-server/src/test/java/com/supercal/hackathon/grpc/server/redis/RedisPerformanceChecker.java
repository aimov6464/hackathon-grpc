//package com.supercal.hackathon.grpc.server;
//
//import com.supercal.hackathon.grpc.proto.Action;
//import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
//import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
//import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
//import com.supercal.hackathon.grpc.server.account.AccountManager;
//import com.supercal.hackathon.grpc.server.account.redis.RedisAccountManager;
//
//import java.util.Random;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.stream.IntStream;
//
//public class RedisPerformanceChecker {
//
//    private static final AtomicLong previousCompleted = new AtomicLong();
//    private static final AtomicLong totalCompleted = new AtomicLong();
//    private static final AccountManager accountManager = new RedisAccountManager(new GrpcServerConfig());
//    private static final Random random = new Random();
//    private static final ExecutorService threadPoolExecutor = Executors.newVirtualThreadPerTaskExecutor();
//
//    public static void main(String[] args) {
////        accountManager.deleteAllAccounts();
////        accountManager.generateAccounts(100_000, 100);
////        System.out.println("Total accounts: " + accountManager.getNumberOfAccounts());
////        accountManager.addToAllAccounts(100);
////        addToAccount(new BetOperation(10, 5));
//        startBetting();
//    }
//
//    private static void addToAccount(BalanceUpdateRequest request) {
//        BalanceUpdateResponse response = accountManager.changeBalance(request);
//        System.out.printf("Balance changed with status %s for account id: %d change: %d \n",
//                response.getStatus().name(), request.getAccountId(), request.getAmount());
//    }
//
//    private static void startBetting() {
//        new Thread(() -> {
//            try {
//                measurePerformance();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }).start();
//
//        int threadSize = 10;
//
//        IntStream.range(0, threadSize).forEach(i ->
//                threadPoolExecutor.submit(() -> {
//                            while (true) {
//                                BalanceUpdateRequestBatch.Builder builder = BalanceUpdateRequestBatch.newBuilder();
//
//                                for(int j = 0; j < 100; j++) {
//                                    int accountId = random.nextInt(100_000);
//                                    int balanceChange = random.nextInt(10) - 5;
//                                    builder.addRequest(BalanceUpdateRequest.newBuilder()
//                                            .setAccountId(accountId)
//                                            .setAction(Action.ACTION_DEBIT)
//                                            .setAmount(balanceChange)
//                                            .build());
//                                }
//
//                                BalanceUpdateRequestBatch batch = builder.build();
//                                accountManager.changeBalance(batch);
//
////                                OperationStatus status = accountManager.changeBalance(new BetOperation(accountId, balanceChange));
//                                // System.out.printf("Balance changed with status %d for account id: %d change: %d \n", status.ordinal(), accountId, balanceChange);
//                                totalCompleted.addAndGet(batch.getRequestCount());
//                            }
//                        }
//                ));
//    }
//
//    private static void measurePerformance() throws InterruptedException {
//
//        long startTime = System.currentTimeMillis();
//
//        if (accountManager.getBalance(10).isPresent()) {
//            System.out.println("Account 10 balance: " + accountManager.getBalance(10));
//        }
//        long time = System.currentTimeMillis();
//        do {
//            Thread.sleep(1000);
//            // Fields for calculating TPS
//            long deltaCompleted = totalCompleted.get() - previousCompleted.get();
//            long duration = System.currentTimeMillis() - time;
//            time = System.currentTimeMillis();
//            log(startTime, duration, deltaCompleted);
//            previousCompleted.set(totalCompleted.get());
//        } while (totalCompleted.get() + totalCompleted.get() < 1_000_000_000);
//    }
//
//    private static void log(long startTime, long duration, long completedRequests) {
//        long tps = (long) (completedRequests / Math.max(1, (double) duration / 1000));
//
//        System.out.println("Time: " + (System.currentTimeMillis() - startTime));
//        System.out.println("Duration: " + duration);
//        System.out.println("Delta: " + completedRequests);
//        System.out.println("Total Completed: " + totalCompleted.get());
//        System.out.println("Transactions Per Second (TPS): " + tps);
//    }
//}
