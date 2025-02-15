package com.supercal.hackathon.grpc.client.account;

import com.supercal.hackathon.grpc.proto.Action;
import com.supercal.hackathon.grpc.proto.BalanceUpdateRequest;
import com.supercal.hackathon.grpc.proto.BalanceUpdateResponse;
import com.supercal.hackathon.grpc.proto.OperationStatus;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Data
public class ClientAccountManager {

    private Map<Integer, Integer> accounts = new ConcurrentHashMap<>();

    public void changeBalance(BalanceUpdateRequest request, BalanceUpdateResponse response) {
        // Get or create account
        Integer balance = accounts.get(request.getAccountId());
        if(balance == null) {
            accounts.put(request.getAccountId(), response.getBalance());
            return;
        }

        // Update balance
        if(response.getStatus() == OperationStatus.OPERATION_STATUS_SUCCESS) {
            balance = balance + (request.getAction() == Action.ACTION_DEBIT ? -request.getAmount() : request.getAmount());
            accounts.put(request.getAccountId(), balance);

            // Check if the client balance is not matching the server balance
            if(balance != response.getBalance()) {
                log.error("Incorrect balance for account={} :: expected={}, got={}",
                        request.getAccountId(), balance, response.getBalance());
            }
        }
    }
}
