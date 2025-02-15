package com.supercal.hackathon.grpc.server.account;

import com.supercal.hackathon.grpc.proto.BalanceUpdateRequestBatch;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponseBatch;
import io.grpc.stub.StreamObserver;

import java.util.Map;

public interface AccountManager {

    Map<Integer, Integer> getAccounts();

    void changeBalance(BalanceUpdateRequestBatch batch, StreamObserver<BalanceUpdateResponseBatch> observer) throws Exception;

    void createAccounts(int numAccounts, int balance) throws Exception;

    default void shutdown() { }
}
