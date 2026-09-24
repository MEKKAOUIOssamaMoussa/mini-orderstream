-- Tables for the shop side. Runs after 00-create-databases.sql.
\connect orders_db

CREATE TABLE orders (
    id           uuid          PRIMARY KEY,
    customer_id  int           NOT NULL,
    amount       numeric(10,2) NOT NULL CHECK (amount > 0),
    status       text          NOT NULL
                 CHECK (status IN ('CREATED', 'PAID', 'SHIPPED', 'CANCELLED')),
    created_at   timestamptz   NOT NULL DEFAULT now(),
    updated_at   timestamptz   NOT NULL DEFAULT now()
);

-- One row per event waiting to be (or already) published to Kafka.
-- No foreign key to orders: this is a log of what happened, never joined back.
CREATE TABLE outbox (
    id          bigserial    PRIMARY KEY,
    event_id    uuid         NOT NULL UNIQUE,
    order_id    uuid         NOT NULL,
    event_type  text         NOT NULL,
    payload     jsonb        NOT NULL,
    created_at  timestamptz  NOT NULL DEFAULT now(),
    sent_at     timestamptz
);

-- The relay only ever looks for unsent rows. This index contains only those,
-- so it stays small no matter how many rows have already been sent.
CREATE INDEX outbox_unsent_idx ON outbox (id) WHERE sent_at IS NULL;
