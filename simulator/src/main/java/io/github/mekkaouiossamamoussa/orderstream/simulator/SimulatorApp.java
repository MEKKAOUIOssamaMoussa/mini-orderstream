package io.github.mekkaouiossamamoussa.orderstream.simulator;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimulatorApp {

    private static final Logger log = LoggerFactory.getLogger(SimulatorApp.class);
    private static volatile boolean running = true;

    public static void main(String[] args) {
        Config config = Config.fromEnv();

        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.dbUrl());
        hikariConfig.setUsername(config.dbUser());
        hikariConfig.setPassword(config.dbPassword());
        hikariConfig.setMaximumPoolSize(2);

        HikariDataSource dataSource = new HikariDataSource(hikariConfig);
        OrderSimulator simulator = new OrderSimulator(dataSource, config);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            mainThread.interrupt();
            dataSource.close();
        }));

        while (running) {
            try {
                simulator.tick();
            } catch (Exception e) {
                log.error("Simulation tick failed", e);
            }

            try {
                Thread.sleep(config.intervalMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
