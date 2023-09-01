package com.example;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SslConfigs;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AivenSSLClient {
    public static void produce() throws IOException {
        File truststoreFile = new File("producer/src/main/java/com/example/ca.pem");
        File keyFile = new File("producer/src/main/java/com/example/service.key");
        File certFile = new File("producer/src/main/java/com/example/service.cert");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "privatelink-1-kafka-eu-dev-westeu-service-eu-dev.aivencloud.com:11307");
        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, FileUtils.readFileToString(truststoreFile, "UTF-8"));
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, FileUtils.readFileToString(keyFile, "UTF-8"));
        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, FileUtils.readFileToString(certFile, "UTF-8"));
//        props.put("ssl.keystore.password", "");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        AivenSSLClient.TestCallback callback = new AivenSSLClient.TestCallback();
        for (long i = 0; i < 1 ; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<>(
                    "internal.de.provisioning-api.request", "key-" + i, "message-"+i );
            producer.send(data, callback);
        }

        producer.close();
    }

    public static void consume() throws IOException {
        final String topic = "internal.de.provisioning-api.request";

        File truststoreFile = new File("producer/src/main/java/com/example/ca.pem");
        File keyFile = new File("producer/src/main/java/com/example/service.key");
        File certFile = new File("producer/src/main/java/com/example/service.cert");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "privatelink-1-kafka-eu-dev-westeu-service-eu-dev.aivencloud.com:11307");
        props.put("security.protocol", "SSL");
        props.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG, FileUtils.readFileToString(truststoreFile, "UTF-8"));
        props.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, "PEM");
        props.put(SslConfigs.SSL_KEYSTORE_KEY_CONFIG, FileUtils.readFileToString(keyFile, "UTF-8"));
        props.put(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG, FileUtils.readFileToString(certFile, "UTF-8"));
//        props.put("ssl.keystore.password", "");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "joonash-test-consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "300000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.MAX_VALUE);
        props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "345000");
        props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, (int) Duration.ofMinutes(10).toMillis());

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1_000_000);
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 150 * 1024 * 1024);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 100 * 1024 * 1024);
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 100 * 1024 * 1024);

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(5));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.println(
                            String.format("Consumed event from topic %s: key = %-10s value = %s", topic, key, value));
                }
            }
        }
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
