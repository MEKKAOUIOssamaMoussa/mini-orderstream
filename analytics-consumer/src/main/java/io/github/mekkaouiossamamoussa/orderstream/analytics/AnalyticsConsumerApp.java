package io.github.mekkaouiossamamoussa.orderstream.analytics;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class AnalyticsConsumerApp {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumerApp.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {
        Config config = Config.fromEnv();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.dbUrl());
        hikariConfig.setUsername(config.dbUser());
        hikariConfig.setPassword(config.dbPassword());
        hikariConfig.setMaximumPoolSize(2);

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        StatsWriter statsWriter = new StatsWriter(dataSource);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        String hostname = System.getenv("HOSTNAME");
        String clientId = (hostname != null && !hostname.isBlank()) ? "analytics-" + hostname : "analytics-local";
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
                int applied = 0;
                int duplicates = 0;

                for (ConsumerRecord<String, String> record : records) {
                    OrderEvent event;
                    try {
                        event = Json.fromJson(record.value());
                    } catch (Exception e) {
                        log.error("Failed to parse event at partition {} offset {}: {}",
                                record.partition(), record.offset(), record.value(), e);
                        continue;
                    }

                    boolean newlyApplied = statsWriter.apply(event);
                    if (newlyApplied) {
                        applied++;
                    } else {
                        duplicates++;
                        log.info("skipped duplicate event {} (partition {} offset {})",
                                event.eventId(), record.partition(), record.offset());
                    }
                }

                if (!records.isEmpty()) {
                    maybeCrash(config, records.count());
                    consumer.commitSync();
                    log.info("batch of {} records: {} applied, {} duplicates",
                            records.count(), applied, duplicates);
                }
            }
            normalStop = true;
        } catch (WakeupException e) {
            normalStop = true;
        } catch (Exception e) {
            log.error("Fatal error in analytics consumer loop", e);
        } finally {
            try {
                consumer.close();
            } catch (Exception e) {
                log.error("Error closing consumer", e);
            }
            try {
                dataSource.close();
            } catch (Exception e) {
                log.error("Error closing data source", e);
            }
            if (!normalStop) {
                running = false;
                System.exit(1);
            }
        }
    }

    private static void maybeCrash(Config config, int recordCount) {
        if (!config.crashOnceBeforeCommit()) {
            return;
        }
        Path crashFile = Path.of("/tmp/analytics-crashed");
        if (!Files.exists(crashFile)) {
            try {
                Files.createFile(crashFile);
            } catch (IOException e) {
                log.error("Failed to create crash marker file {}", crashFile, e);
            }
            log.warn("crash test: halting after writing {} records, before committing offsets", recordCount);
            Runtime.getRuntime().halt(1);
        }
    }
}
