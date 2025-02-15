package com.supercal.hackathon.grpc.client.grpc;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
@Data
public class GrpcConnection {

    private final ManagedChannel channel;
    private final Lock lock;
    private final Condition connectionReady;
    private final List<GrpcConnectionStateListener> stateListeners;
    private ConnectivityState state;

    public GrpcConnection(ManagedChannel channel) {
        this.channel = channel;
        this.lock = new ReentrantLock();
        this.connectionReady = lock.newCondition();
        this.state = channel.getState(true);
        this.stateListeners = new ArrayList<>();

        channel.notifyWhenStateChanged(ConnectivityState.IDLE, this::onStateChange);
    }

    private void onStateChange() {
        lock.lock();

        try {
            state = channel.getState(true);
            log.info("Connection state changed to {}", state);
            if (state == ConnectivityState.READY) {
                TimeUnit.SECONDS.sleep(1);
                stateListeners.forEach(GrpcConnectionStateListener::onReconnect);
                connectionReady.signalAll();
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }

        channel.notifyWhenStateChanged(state, this::onStateChange);
    }

    public void addStateChangeListener(GrpcConnectionStateListener listener) {
        stateListeners.add(listener);
    }

    public void whenReady() throws InterruptedException {
        lock.lock();

        try {
            while (state != ConnectivityState.READY) {
                connectionReady.await();
            }
        } finally {
            lock.unlock();
        }
    }

    public void shutdown() {
        channel.shutdown();

        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Interrupted");
        }
    }
}
