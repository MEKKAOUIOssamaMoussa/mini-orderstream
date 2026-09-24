package io.github.mekkaouiossamamoussa.orderstream.analytics;

import io.github.mekkaouiossamamoussa.orderstream.common.OrderEvent;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

public class StatsWriter {

    private static final String INSERT_PROCESSED_SQL =
            "INSERT INTO processed_events (event_id) VALUES (?) ON CONFLICT (event_id) DO NOTHING";

    private static final String UPSERT_STATS_SQL =
            "INSERT INTO customer_stats (customer_id, orders_created, orders_paid, " +
            "    orders_shipped, orders_cancelled, total_paid) " +
            "VALUES (?, ?, ?, ?, ?, ?) " +
            "ON CONFLICT (customer_id) DO UPDATE SET " +
            "    orders_created   = customer_stats.orders_created   + EXCLUDED.orders_created, " +
            "    orders_paid      = customer_stats.orders_paid      + EXCLUDED.orders_paid, " +
            "    orders_shipped   = customer_stats.orders_shipped   + EXCLUDED.orders_shipped, " +
            "    orders_cancelled = customer_stats.orders_cancelled + EXCLUDED.orders_cancelled, " +
            "    total_paid       = customer_stats.total_paid       + EXCLUDED.total_paid, " +
            "    updated_at       = now()";

    private final DataSource dataSource;

    public StatsWriter(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource);
    }

    public boolean apply(OrderEvent event) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                try (PreparedStatement insertStmt = conn.prepareStatement(INSERT_PROCESSED_SQL)) {
                    insertStmt.setObject(1, event.eventId());
                    int rows = insertStmt.executeUpdate();
                    if (rows == 0) {
                        conn.rollback();
                        return false;
                    }
                }

                int created = 0;
                int paid = 0;
                int shipped = 0;
                int cancelled = 0;
                BigDecimal totalPaid = BigDecimal.ZERO;

                switch (event.status()) {
                    case CREATED -> created = 1;
                    case PAID -> {
                        paid = 1;
                        totalPaid = event.amount();
                    }
                    case SHIPPED -> shipped = 1;
                    case CANCELLED -> cancelled = 1;
                }

                try (PreparedStatement upsertStmt = conn.prepareStatement(UPSERT_STATS_SQL)) {
                    upsertStmt.setInt(1, event.customerId());
                    upsertStmt.setInt(2, created);
                    upsertStmt.setInt(3, paid);
                    upsertStmt.setInt(4, shipped);
                    upsertStmt.setInt(5, cancelled);
                    upsertStmt.setBigDecimal(6, totalPaid);
                    upsertStmt.executeUpdate();
                }

                conn.commit();
                return true;
            } catch (SQLException | RuntimeException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackEx) {
                    e.addSuppressed(rollbackEx);
                }
                throw e;
            }
        }
    }
}
