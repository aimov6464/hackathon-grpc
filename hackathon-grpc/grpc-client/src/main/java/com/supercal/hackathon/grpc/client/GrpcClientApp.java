package com.supercal.hackathon.grpc.client;

import com.supercal.hackathon.grpc.client.account.AccountClient;
import com.supercal.hackathon.grpc.client.account.AccountClientCallback;
import com.supercal.hackathon.grpc.client.account.ClientAccountManager;
import com.supercal.hackathon.grpc.client.grpc.GrpcChannelManager;
import com.supercal.hackathon.grpc.client.testdata.BatchRequestGenerator;
import com.supercal.hackathon.grpc.commons.account.AccountFileUtil;
import com.supercal.hackathon.grpc.commons.monitoring.Monitoring;
import com.supercal.hackathon.grpc.commons.properties.PropertiesReader;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GrpcClientApp {

    private GrpcClientConfig config;
    private AccountClient accountClient;
    private BatchRequestGenerator generator;

    public static void main(String[] args) throws Exception {
        GrpcClientApp grpcClientApp = new GrpcClientApp();
        grpcClientApp.start(args.length == 0 ? "default" : args[0]);
    }

    public void start(String phase) throws Exception {
        // Init
        Properties properties = PropertiesReader.readProperties("config.properties");
        config = new GrpcClientConfig(properties);

        GrpcChannelManager channelManager = new GrpcChannelManager(config);
        ClientAccountManager accountManager = new ClientAccountManager();
        AccountClientCallback callback = new AccountClientCallback(accountManager);
        accountClient = new AccountClient(config, channelManager, callback);

        // Start generating batches and send
        generator = new BatchRequestGenerator(config);

        switch (phase) {
            case "phase1":
                Thread.startVirtualThread(this::phase1);
                break;
            case "phase2":
                Thread.startVirtualThread(this::phase2);
                break;
            case "phase3":
                Thread.startVirtualThread(this::phase3);
                break;
            default:
                Thread.startVirtualThread(this::noPhase);
                break;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown hook triggered...");
            generator.shutdown();
            System.out.println("RequestGenerator shut down");

            accountClient.shutdown();
            System.out.println("AccountClient shut down");

            channelManager.shutdown();
            System.out.println("ChannelManager client shut down");

            log.info("Writing accounts to disk...");
            AccountFileUtil.writeToFile(new TreeMap<>(accountManager.getAccounts()) ,"account-client.txt");

            log.info("Shutdown complete");
        }));

        // Collect metrics
        AccountClientCallback.Stats stats = callback.getStats();
        long startTime = System.currentTimeMillis();
        long time = startTime;
        long previousCompleted = 0;

        while (!Thread.interrupted()) {
            TimeUnit.MILLISECONDS.sleep(1000);

            // Fields for calculating TPS
            long deltaCompleted = stats.getFinished().get() - previousCompleted;
            long duration = System.currentTimeMillis() - time;
            time = System.currentTimeMillis();

            log(stats, startTime, duration, deltaCompleted);
            previousCompleted = stats.getFinished().get();
        }
    }

    private void noPhase() {
        generator.start(accountClient::addBatch);
    }

    private void phase1() {
        int tps = config.getPhase1TpsStart();
        accountClient.setTps(tps);
        log.info("[PHASE1]: Starting :: tps={}", tps);

        // Start generating requests
        generator.start(accountClient::addBatch);

        do {
            try {
                // Sleep
                TimeUnit.SECONDS.sleep(config.getPhase1Sleep());
            } catch (InterruptedException e) {
                log.info("Interrupted");
                break;
            }

            // Increase tps
            tps = tps + config.getPhase1TpsIncrement();
            log.info("[PHASE1]: increasing tps to {}", tps);
            accountClient.setTps(tps);
        } while(tps <= config.getPhase1TpsEnd());

        // Done with phase
        generator.shutdown();
        log.info("[PHASE1]: phase is completed");
    }

    private void phase2() {
        int tps = config.getPhase2TpsStart();
        accountClient.setTps(tps);
        log.info("[PHASE2]: Starting :: tps={}", tps);

        // Start generating requests
        generator.start(accountClient::addBatch);

        try {
            // Sleep
            TimeUnit.SECONDS.sleep(config.getPhase2Sleep());
        } catch (InterruptedException e) {
            log.info("Interrupted");
        }

        // Done with phase
        generator.shutdown();
        log.info("[PHASE2]: phase is completed");
    }

    private void phase3() {
        int tps = config.getPhase3TpsStart();
        accountClient.setTps(tps);
        log.info("[PHASE3]: Starting :: tps={}", tps);

        // Start generating requests
        generator.start(accountClient::addBatch);

        do {
            try {
                // Sleep
                TimeUnit.SECONDS.sleep(config.getPhase3Sleep());
            } catch (InterruptedException e) {
                log.info("Interrupted");
                break;
            }

            // Increase tps
            tps = tps + config.getPhase3TpsIncrement();
            log.info("[PHASE3]: increasing tps to {}", tps);
            accountClient.setTps(tps);
        } while(!Thread.interrupted());

        // Done with phase
        generator.shutdown();
        log.info("[PHASE3]: phase is completed");
    }

    private void log(AccountClientCallback.Stats stats, long startTime, long duration, long completedRequests) {
        // Calculate TPS
        long tps = (long)(completedRequests / Math.max(1, (double)duration / 1000));
        long time =  (System.currentTimeMillis() - startTime) / 1000;

        log.info("time={} sent={} finished={} retry={} failed={} duplicate={} cpu={} ram={} tps={}",
                time, stats.getSent(), stats.getFinished(), stats.getRetry(), stats.getFailed(), stats.getDuplicate(), Monitoring.getCpu(), Monitoring.getRam(), tps);
    }
}
