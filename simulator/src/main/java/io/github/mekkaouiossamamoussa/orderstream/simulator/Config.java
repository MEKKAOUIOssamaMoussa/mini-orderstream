package io.github.mekkaouiossamamoussa.orderstream.simulator;

public record Config(
        String dbUrl,
        String dbUser,
        String dbPassword,
        long intervalMs,
        int customerCount
) {
    private static final String DEFAULT_DB_URL = "jdbc:postgresql://localhost:5433/orders_db";
    private static final String DEFAULT_DB_USER = "orderstream";
    private static final String DEFAULT_DB_PASSWORD = "orderstream";
    private static final long DEFAULT_SIM_INTERVAL_MS = 1000L;
    private static final int DEFAULT_SIM_CUSTOMERS = 20;

    public static Config fromEnv() {
        String dbUrl = getEnvOrDefault("DB_URL", DEFAULT_DB_URL);
        String dbUser = getEnvOrDefault("DB_USER", DEFAULT_DB_USER);
        String dbPassword = getEnvOrDefault("DB_PASSWORD", DEFAULT_DB_PASSWORD);
        long intervalMs = getEnvOrDefaultLong("SIM_INTERVAL_MS", DEFAULT_SIM_INTERVAL_MS);
        int customerCount = getEnvOrDefaultInt("SIM_CUSTOMERS", DEFAULT_SIM_CUSTOMERS);

        return new Config(dbUrl, dbUser, dbPassword, intervalMs, customerCount);
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
