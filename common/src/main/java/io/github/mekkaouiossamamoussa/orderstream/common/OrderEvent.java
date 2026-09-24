package io.github.mekkaouiossamamoussa.orderstream.common;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderEvent(
        UUID eventId,
        EventType eventType,
        int schemaVersion,
        UUID orderId,
        int customerId,
        BigDecimal amount,
        OrderStatus status,
        Instant occurredAt
) {
    public static final int CURRENT_SCHEMA_VERSION = 1;
}
