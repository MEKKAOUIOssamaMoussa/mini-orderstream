package io.github.mekkaouiossamamoussa.orderstream.audit;

import io.github.mekkaouiossamamoussa.orderstream.common.Json;
import io.github.mekkaouiossamamoussa.orderstream.common.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class AuditConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(AuditConsumerApp.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {
        Config config = Config.fromEnv();

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String hostname = System.getenv("HOSTNAME");
        String clientId = (hostname != null && !hostname.isBlank()) ? "audit-" + hostname : "audit-local";
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(List.of(config.topic()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                List<Integer> parts = partitions.stream()
                        .map(TopicPartition::partition)
                        .sorted()
                        .toList();
                log.info("revoked partitions: {}", parts);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                List<Integer> parts = partitions.stream()
                        .map(TopicPartition::partition)
                        .sorted()
                        .toList();
                log.info("assigned partitions: {}", parts);
            }
        });

        BufferedWriter writer;
        try {
            Path auditPath = Path.of(config.auditFile());
            if (auditPath.getParent() != null) {
                Files.createDirectories(auditPath.getParent());
            }
            writer = Files.newBufferedWriter(auditPath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to open audit file: {}", config.auditFile(), e);
            consumer.close();
            System.exit(1);
            return;
        }

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (!running) {
                return;
            }
            running = false;
            consumer.wakeup();
            try {
                mainThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        boolean normalStop = false;
        try {
            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    OrderEvent event;
                    try {
                        event = Json.fromJson(record.value());
                    } catch (Exception e) {
                        writer.write(Instant.now() + " partition=" + record.partition() + " offset=" + record.offset()
                                + " UNPARSEABLE " + record.value());
                        writer.newLine();
                        continue;
                    }

                    writer.write(Instant.now() + " partition=" + record.partition() + " offset=" + record.offset()
                            + " event=" + event.eventId() + " order=" + event.orderId()
                            + " type=" + event.eventType().jsonName() + " status=" + event.status());
                    writer.newLine();
                }

                if (!records.isEmpty()) {
                    writer.flush();
                    consumer.commitSync();
                    log.info("wrote {} lines", records.count());
                }
            }
            normalStop = true;
        } catch (WakeupException e) {
            normalStop = true;
        } catch (Exception e) {
            log.error("Fatal error in audit consumer loop", e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
            try {
                writer.close();
            } catch (Exception e) {
                log.error("Error closing writer", e);
            }
            if (!normalStop) {
                running = false;
                System.exit(1);
            }
        }
    }
}
