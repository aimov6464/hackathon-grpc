package com.supercal.hackathon.grpc.server.account;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Map;

public class AccountCache {

    public static final int defaultReturnValue = Integer.MAX_VALUE;
    private final Int2IntOpenHashMap cache;

    public AccountCache(int size) {
        cache = new Int2IntOpenHashMap(size);
        cache.defaultReturnValue(defaultReturnValue);
    }

    public int get(int accountId) {
        return cache.get(accountId);
    }

    public void put(int accountId, int balance) {
        cache.put(accountId, balance);
    }

    public void putAll(Map<Integer, Integer> accounts) {
        cache.putAll(accounts);
    }

    public boolean containsKey(int accountId) {
        return cache.containsKey(accountId);
    }

    public Int2IntOpenHashMap getMap() {
        return cache;
    }
}
