package com.example;

import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.util.Properties;

public class AivenClient {
    public static void run(){
        String sasl_username = "";
        String sasl_password = "";
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);
        File truststoreFile = new File("producer/src/main/java/com/example/client.truststore.jks");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "provisioner-dev-replicator-dev.aivencloud.com:18560");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
//        props.put("ssl.truststore.type", "JKS");
//        props.put("ssl.truststore.location", truststoreFile.getAbsolutePath());
//        props.put("ssl.truststore.password", "relexsolutions");

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        AivenClient.TestCallback callback = new AivenClient.TestCallback();
        for (long i = 0; i < 1 ; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<>(
                    "internal.de.availability.ci-pipeline", "key-" + i, "message-"+i );
            producer.send(data, callback);
        }

        producer.close();
    }

    private static class TestCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null) {
                System.out.println("Error while producing message to topic :" + recordMetadata);
                e.printStackTrace();
            } else {
                String message = String.format("sent message to topic:%s partition:%s  offset:%s", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                System.out.println(message);
            }
        }
    }
}
