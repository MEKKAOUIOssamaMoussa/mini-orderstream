package io.github.mekkaouiossamamoussa.orderstream.audit;

public record Config(
        String kafkaBootstrapServers,
        String topic,
        String groupId,
        String auditFile
) {
    private static final String DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9094";
    private static final String DEFAULT_TOPIC = "orders.events";
    private static final String DEFAULT_GROUP_ID = "audit";
    private static final String DEFAULT_AUDIT_FILE = "audit.log";

    public static Config fromEnv() {
        String kafkaBootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_KAFKA_BOOTSTRAP_SERVERS);
        String topic = getEnvOrDefault("TOPIC", DEFAULT_TOPIC);
        String groupId = getEnvOrDefault("GROUP_ID", DEFAULT_GROUP_ID);
        String auditFile = getEnvOrDefault("AUDIT_FILE", DEFAULT_AUDIT_FILE);

        return new Config(kafkaBootstrapServers, topic, groupId, auditFile);
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
