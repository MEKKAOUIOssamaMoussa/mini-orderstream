package io.github.mekkaouiossamamoussa.orderstream.relay;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

public class RelayApp {

    private static final Logger log = LoggerFactory.getLogger(RelayApp.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {
        Config config = Config.fromEnv();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.dbUrl());
        hikariConfig.setUsername(config.dbUser());
        hikariConfig.setPassword(config.dbPassword());
        hikariConfig.setMaximumPoolSize(2);

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "relay");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        OutboxRelay relay = new OutboxRelay(dataSource, producer, config);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            try {
                mainThread.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        try {
            while (running) {
                try {
                    int n = relay.publishBatch();
                    if (n == 0 && running) {
                        Thread.sleep(config.pollIntervalMs());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Relay batch publication failed", e);
                    if (running) {
                        try {
                            Thread.sleep(config.pollIntervalMs());
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }
        } finally {
            producer.close(Duration.ofSeconds(5));
            dataSource.close();
        }
    }
}
