-- Tables for the analytics side. Runs after 01-orders-schema.sql.
\connect analytics_db

-- One row per customer, updated by the analytics consumer.
CREATE TABLE customer_stats (
    customer_id       int            PRIMARY KEY,
    orders_created    int            NOT NULL DEFAULT 0,
    orders_paid       int            NOT NULL DEFAULT 0,
    orders_shipped    int            NOT NULL DEFAULT 0,
    orders_cancelled  int            NOT NULL DEFAULT 0,
    total_paid        numeric(12,2)  NOT NULL DEFAULT 0,
    updated_at        timestamptz    NOT NULL DEFAULT now()
);

-- Every event id already counted. Inserting a duplicate id fails,
-- which is how the consumer recognizes a redelivered event.
CREATE TABLE processed_events (
    event_id      uuid         PRIMARY KEY,
    processed_at  timestamptz  NOT NULL DEFAULT now()
);
