package io.github.mekkaouiossamamoussa.orderstream.simulator;

import io.github.mekkaouiossamamoussa.orderstream.common.EventType;
import io.github.mekkaouiossamamoussa.orderstream.common.Json;
import io.github.mekkaouiossamamoussa.orderstream.common.OrderEvent;
import io.github.mekkaouiossamamoussa.orderstream.common.OrderStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

public class OrderSimulator {

    private static final Logger log = LoggerFactory.getLogger(OrderSimulator.class);

    private static final String SELECT_OPEN_ORDER_SQL =
            "SELECT id, customer_id, amount, status FROM orders " +
            "WHERE status IN ('CREATED','PAID') ORDER BY random() LIMIT 1";

    private static final String INSERT_ORDER_SQL =
            "INSERT INTO orders (id, customer_id, amount, status) VALUES (?, ?, ?, ?)";

    private static final String UPDATE_ORDER_SQL =
            "UPDATE orders SET status = ?, updated_at = now() WHERE id = ? AND status = ?";

    private static final String INSERT_OUTBOX_SQL =
            "INSERT INTO outbox (event_id, order_id, event_type, payload) VALUES (?, ?, ?, ?::jsonb)";

    private final DataSource dataSource;
    private final Config config;
    private final Random random = new Random();

    public OrderSimulator(DataSource dataSource, Config config) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.config = Objects.requireNonNull(config);
    }

    public void tick() throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                boolean createNew = random.nextDouble() < 0.40;
                if (!createNew) {
                    Optional<OpenOrder> openOrder = findRandomOpenOrder(conn);
                    if (openOrder.isPresent()) {
                        advanceOrder(conn, openOrder.get());
                        return;
                    }
                }
                createNewOrder(conn);
            } catch (Exception e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw e;
            }
        }
    }

    private void createNewOrder(Connection conn) throws SQLException {
        UUID orderId = UUID.randomUUID();
        int customerId = 1 + random.nextInt(config.customerCount());
        int cents = 500 + random.nextInt(30000 - 500 + 1);
        BigDecimal amount = BigDecimal.valueOf(cents, 2);
        OrderStatus status = OrderStatus.CREATED;

        try (PreparedStatement orderStmt = conn.prepareStatement(INSERT_ORDER_SQL)) {
            orderStmt.setObject(1, orderId);
            orderStmt.setInt(2, customerId);
            orderStmt.setBigDecimal(3, amount);
            orderStmt.setString(4, status.name());
            orderStmt.executeUpdate();
        }

        OrderEvent event = new OrderEvent(
                UUID.randomUUID(),
                EventType.ORDER_CREATED,
                OrderEvent.CURRENT_SCHEMA_VERSION,
                orderId,
                customerId,
                amount,
                status,
                Instant.now()
        );

        insertOutbox(conn, event);
        conn.commit();
        log.info("created order {} customer={} amount={}", orderId, customerId, amount);
    }

    private void advanceOrder(Connection conn, OpenOrder order) throws SQLException {
        OrderStatus newStatus = switch (order.status()) {
            case CREATED -> (random.nextDouble() < 0.80) ? OrderStatus.PAID : OrderStatus.CANCELLED;
            case PAID -> OrderStatus.SHIPPED;
            default -> throw new IllegalStateException("Unexpected status for open order: " + order.status());
        };

        try (PreparedStatement updateStmt = conn.prepareStatement(UPDATE_ORDER_SQL)) {
            updateStmt.setString(1, newStatus.name());
            updateStmt.setObject(2, order.id());
            updateStmt.setString(3, order.status().name());
            int updatedCount = updateStmt.executeUpdate();
            if (updatedCount != 1) {
                conn.rollback();
                log.warn("Expected 1 row updated for order {} with status {}, got {}",
                        order.id(), order.status(), updatedCount);
                return;
            }
        }

        OrderEvent event = new OrderEvent(
                UUID.randomUUID(),
                EventType.ORDER_STATUS_CHANGED,
                OrderEvent.CURRENT_SCHEMA_VERSION,
                order.id(),
                order.customerId(),
                order.amount(),
                newStatus,
                Instant.now()
        );

        insertOutbox(conn, event);
        conn.commit();
        log.info("order {} {} -> {}", order.id(), order.status(), newStatus);
    }

    private void insertOutbox(Connection conn, OrderEvent event) throws SQLException {
        try (PreparedStatement outboxStmt = conn.prepareStatement(INSERT_OUTBOX_SQL)) {
            outboxStmt.setObject(1, event.eventId());
            outboxStmt.setObject(2, event.orderId());
            outboxStmt.setString(3, event.eventType().jsonName());
            outboxStmt.setString(4, Json.toJson(event));
            outboxStmt.executeUpdate();
        }
    }

    private Optional<OpenOrder> findRandomOpenOrder(Connection conn) throws SQLException {
        try (PreparedStatement selectStmt = conn.prepareStatement(SELECT_OPEN_ORDER_SQL);
             ResultSet rs = selectStmt.executeQuery()) {
            if (rs.next()) {
                UUID id = rs.getObject("id", UUID.class);
                int customerId = rs.getInt("customer_id");
                BigDecimal amount = rs.getBigDecimal("amount");
                OrderStatus status = OrderStatus.valueOf(rs.getString("status"));
                return Optional.of(new OpenOrder(id, customerId, amount, status));
            }
        }
        return Optional.empty();
    }

    private record OpenOrder(UUID id, int customerId, BigDecimal amount, OrderStatus status) {}
}
