package com.supercal.hackathon.grpc.commons.account;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class AccountFileUtil {

    public static void writeToFile(Map<Integer, Integer> accounts, String outputFilePath) {
        System.out.println("Writing accounts to file for verification = " + Paths.get(outputFilePath));

        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(outputFilePath), StandardCharsets.UTF_8)) {
            for (Map.Entry<Integer, Integer> entry : accounts.entrySet()) {
                writer.write(entry.getKey() + "_" + entry.getValue());
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("done");
    }
}
