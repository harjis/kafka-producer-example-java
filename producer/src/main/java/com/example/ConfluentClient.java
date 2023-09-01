package com.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConfluentClient {
    public static void produce(){
        String sasl_username = "";
        String sasl_password = "";
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-1wvvj.westeurope.azure.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        ConfluentClient.TestCallback callback = new ConfluentClient.TestCallback();
        for (long i = 0; i < 1 ; i++) {
            ProducerRecord<String, String> data = new ProducerRecord<>(
                    "internal.de.provisioning-api.request", "key-" + i, "message-"+i );
            producer.send(data, callback);
        }

        producer.close();
    }

    public static void consume() {
        final String topic = "internal.de.provisioning-api.request";
        String sasl_username = "";
        String sasl_password = "";
        String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";";
        String jaasConfig = String.format(jaasTemplate, sasl_username, sasl_password);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-1wvvj.westeurope.azure.confluent.cloud:9092");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config", jaasConfig);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "joonash-test-consumer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // 210000 does not work
        // 209000 fails kind of but the message also is delivered
        // 208500 fails kind of but the message also is delivered
        // 208400 fails kind of but the message also is delivered
        // 208300 works
        // 208000 works
        // 207000 works
        // 206000 works
        // 205000 works
        // 200000 works
        props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "210000");
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
                if (!records.isEmpty()) {
                    break;
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
