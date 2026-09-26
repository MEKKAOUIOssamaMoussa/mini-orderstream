package io.github.mekkaouiossamamoussa.orderstream.relay;

public record Config(
        String dbUrl,
        String dbUser,
        String dbPassword,
        String kafkaBootstrapServers,
        String topic,
        int batchSize,
        long pollIntervalMs,
        boolean crashOnceAfterSend
) {
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5433/orders_db";
    private static final String DEFAULT_DB_USER = "orderstream";
    private static final String DEFAULT_DB_PASSWORD = "orderstream";
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9094";
    private static final String DEFAULT_TOPIC = "orders.events";
    private static final int DEFAULT_BATCH_SIZE = 100;
    private static final long DEFAULT_POLL_INTERVAL_MS = 500L;

    public static Config fromEnv() {
        String dbUrl = getEnvOrDefault("DB_URL", DEFAULT_DB_URL);
        String dbUser = getEnvOrDefault("DB_USER", DEFAULT_DB_USER);
        String dbPassword = getEnvOrDefault("DB_PASSWORD", DEFAULT_DB_PASSWORD);
        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        String topic = getEnvOrDefault("TOPIC", DEFAULT_TOPIC);
        int batchSize = getEnvOrDefaultInt("RELAY_BATCH_SIZE", DEFAULT_BATCH_SIZE);
        long pollIntervalMs = getEnvOrDefaultLong("RELAY_POLL_INTERVAL_MS", DEFAULT_POLL_INTERVAL_MS);
        boolean crashOnceAfterSend = Boolean.parseBoolean(System.getenv("RELAY_CRASH_ONCE_AFTER_SEND"));

        return new Config(dbUrl, dbUser, dbPassword, kafkaBootstrapServers, topic, batchSize, pollIntervalMs, crashOnceAfterSend);
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }

    private static long getEnvOrDefaultLong(String key, long defaultValue) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) {
            try {
                return Long.parseLong(val.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }

    private static int getEnvOrDefaultInt(String key, int defaultValue) {
        String val = System.getenv(key);
        if (val != null && !val.isBlank()) {
            try {
                return Integer.parseInt(val.trim());
            } catch (NumberFormatException ignored) {
            }
        }
        return defaultValue;
    }
}
