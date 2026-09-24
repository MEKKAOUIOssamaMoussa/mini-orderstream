package io.github.mekkaouiossamamoussa.orderstream.analytics;

public record Config(
        String dbUrl,
        String dbUser,
        String dbPassword,
        String kafkaBootstrapServers,
        String topic,
        String groupId
) {
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5433/analytics_db";
    private static final String DEFAULT_DB_USER = "orderstream";
    private static final String DEFAULT_DB_PASSWORD = "orderstream";
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9094";
    private static final String DEFAULT_TOPIC = "orders.events";
    private static final String DEFAULT_GROUP_ID = "analytics";

    public static Config fromEnv() {
        String dbUrl = getEnvOrDefault("DB_URL", DEFAULT_DB_URL);
        String dbUser = getEnvOrDefault("DB_USER", DEFAULT_DB_USER);
        String dbPassword = getEnvOrDefault("DB_PASSWORD", DEFAULT_DB_PASSWORD);
        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        String topic = getEnvOrDefault("TOPIC", DEFAULT_TOPIC);
        String groupId = getEnvOrDefault("GROUP_ID", DEFAULT_GROUP_ID);

        return new Config(dbUrl, dbUser, dbPassword, kafkaBootstrapServers, topic, groupId);
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
