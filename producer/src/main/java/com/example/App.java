package com.example;


import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class App {

    public static void main(String[] args) throws IOException {
//        AivenClient.run();
//        ConfluentClient.produce();
        ConfluentClient.consume();
//        AivenSSLClient.produce();
//        AivenSSLClient.consume();
//        AivenDescribeTopic.run();
    }
}