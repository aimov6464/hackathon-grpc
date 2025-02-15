package com.supercal.hackathon.grpc.commons.properties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesReader {

    public static Properties readProperties(String path) throws IOException {
        Properties properties;

        // From resources
        String propertiesFromResources = "config.properties";
        try (InputStream input = PropertiesReader.class.getClassLoader().getResourceAsStream(propertiesFromResources)) {
            properties = new Properties();
            if (input != null){
                properties.load(input);
                System.out.println("Found properties from resource: " + propertiesFromResources);
            } else{
                System.out.println(propertiesFromResources + " from resource not found");
            }
        }

        // From file system
        File file = new File(path);
        if (file.exists()) {
            try (InputStream input = new FileInputStream(path)) {
                properties = new Properties();
                properties.load(input);
                System.out.println("Found properties from file system: " + path);
            }
        }

        return properties;
    }
}
