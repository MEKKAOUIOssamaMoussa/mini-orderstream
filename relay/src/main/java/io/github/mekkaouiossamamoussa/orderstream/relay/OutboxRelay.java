package io.github.mekkaouiossamamoussa.orderstream.relay;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;

public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private static final String SELECT_UNSENT_SQL =
            "SELECT id, event_id, order_id, payload::text FROM outbox " +
            "WHERE sent_at IS NULL ORDER BY id LIMIT ?";

    private static final String UPDATE_SENT_SQL =
            "UPDATE outbox SET sent_at = now() WHERE id = ANY(?)";

    private final DataSource dataSource;
    private final Producer<String, String> producer;
    private final Config config;

    public OutboxRelay(DataSource dataSource, Producer<String, String> producer, Config config) {
        this.dataSource = Objects.requireNonNull(dataSource);
        this.producer = Objects.requireNonNull(producer);
        this.config = Objects.requireNonNull(config);
    }

    public int publishBatch() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            try {
                List<PendingRow> pendingRows = new ArrayList<>();
                try (PreparedStatement selectStmt = conn.prepareStatement(SELECT_UNSENT_SQL)) {
                    selectStmt.setInt(1, config.batchSize());
                    try (ResultSet rs = selectStmt.executeQuery()) {
                        while (rs.next()) {
                            long id = rs.getLong(1);
                            UUID eventId = rs.getObject(2, UUID.class);
                            UUID orderId = rs.getObject(3, UUID.class);
                            String payloadText = rs.getString(4);

                            Future<RecordMetadata> future = producer.send(
                                    new ProducerRecord<>(config.topic(), orderId.toString(), payloadText)
                            );
                            pendingRows.add(new PendingRow(id, eventId, orderId, future));
                        }
                    }
                }

                if (pendingRows.isEmpty()) {
                    conn.commit();
                    return 0;
                }

                List<Long> sentIds = new ArrayList<>();
                for (PendingRow row : pendingRows) {
                    RecordMetadata metadata;
                    try {
                        metadata = row.future().get();
                    } catch (Exception e) {
                        log.error("Failed to publish event {} order {}", row.eventId(), row.orderId(), e);
                        break;
                    }
                    sentIds.add(row.id());
                    log.info("published event {} order {} -> partition {} offset {}",
                            row.eventId(), row.orderId(), metadata.partition(), metadata.offset());
                }

                maybeCrash(sentIds.size());

                if (!sentIds.isEmpty()) {
                    try (PreparedStatement updateStmt = conn.prepareStatement(UPDATE_SENT_SQL)) {
                        Array idArray = conn.createArrayOf("bigint", sentIds.toArray(new Long[0]));
                        updateStmt.setArray(1, idArray);
                        updateStmt.executeUpdate();
                    }
                }

                conn.commit();
                return sentIds.size();
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

    private void maybeCrash(int confirmedCount) {
        if (!config.crashOnceAfterSend() || confirmedCount == 0) {
            return;
        }
        Path crashFile = Path.of("/tmp/relay-crashed");
        if (!Files.exists(crashFile)) {
            try {
                Files.createFile(crashFile);
            } catch (IOException e) {
                log.error("Failed to create crash marker file {}", crashFile, e);
            }
            log.warn("crash test: halting after Kafka confirmed {} events, before marking them sent", confirmedCount);
            Runtime.getRuntime().halt(1);
        }
    }

    private record PendingRow(long id, UUID eventId, UUID orderId, Future<RecordMetadata> future) {}
}
