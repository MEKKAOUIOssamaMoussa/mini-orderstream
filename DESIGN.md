# mini-orderstream: design

A small event pipeline built to get hands-on experience with Kafka. An order
simulator writes to Postgres, a relay publishes those changes to Kafka, and two
consumer groups process the same events for different purposes. One broker,
everything runs locally with Docker Compose.

This document was written before the code. It gets updated as each stage is
built; when a decision changes, the reason goes in the Findings section at the
bottom instead of silently replacing the old text.

## Architecture

```
                 +-------------- Postgres (one container) ---------------+
                 |                                                       |
+-----------+    |  orders_db                   analytics_db             |
| simulator |--->|   - orders                    - customer_stats        |
+-----------+    |   - outbox  <--+              - processed_events  <-+ |
                 +----------------|------------------------------------|-+
                                  | 1. read unsent rows                |
                            +-----+-----+                              |
                            |   relay   |  2. publish to Kafka         |
                            +-----+-----+  3. set sent_at              |
                                  v                                    |
                 +------ Kafka (1 broker, KRaft) ------+               |
                 | topic orders.events, 3 partitions   |               |
                 | key = order_id                      |               |
                 +-------+---------------------+-------+               |
                         |                     |                       |
              group "analytics"           group "audit"                |
          +--------------+--------------+ +--------------+             |
          | analytics #1 | analytics #2 | | audit-logger |--> log file |
          +------+-------+------+-------+ +--------------+             |
                 +--------------+--------------------------------------+
```

## Components

**simulator**: roughly once per second, either creates an order for a random
customer (pool of 20 customers) or moves an existing order to its next status.
Each change runs in one transaction that writes the `orders` row and inserts an
`outbox` row.

**relay**: polls `outbox` for rows where `sent_at` is null, oldest first,
publishes each one to `orders.events` with the order id as key, waits for Kafka
to acknowledge, then sets `sent_at`. Only one relay instance runs.

**analytics-consumer**: consumer group `analytics`, runs as 2 instances.
Maintains `customer_stats`. For each event, in one transaction: insert the
event id into `processed_events` (skip the event if it is already there), then
update the customer's row. Offsets are committed by hand after that
transaction commits.

**audit-consumer**: consumer group `audit`. Appends one line per received event
to a log file on a Docker volume. No duplicate check (see decisions).

**postgres**: one container with two databases. The shop side only uses
`orders_db`, the analytics side only uses `analytics_db`. Kafka is the only
link between them.

**kafka**: one node in KRaft mode acting as both broker and controller. The
topic is created explicitly with 3 partitions and replication factor 1;
automatic topic creation is turned off.

## Order lifecycle

```
CREATED -> PAID -> SHIPPED
CREATED -> CANCELLED
```

## Tables

orders_db:

- `orders(id uuid pk, customer_id int, amount numeric(10,2), status text, created_at, updated_at)`
- `outbox(id bigserial pk, event_id uuid unique, order_id uuid, event_type text, payload jsonb, created_at, sent_at null)`

`outbox` has two ids on purpose. `id` is an increasing number the relay uses to
read rows in the order they were written. `event_id` is the identity of the
event itself and travels inside the Kafka message.

analytics_db:

- `customer_stats(customer_id pk, orders_created, orders_paid, orders_shipped, orders_cancelled, total_paid)`
- `processed_events(event_id uuid pk, processed_at)`

## Event format

JSON. Both event types (`OrderCreated`, `OrderStatusChanged`) use the same shape:

```json
{
  "eventId": "6f1c...",
  "eventType": "OrderStatusChanged",
  "schemaVersion": 1,
  "orderId": "a83e...",
  "customerId": 7,
  "amount": 80.00,
  "status": "PAID",
  "occurredAt": "2026-09-23T14:02:11Z"
}
```

## Decisions

### KRaft, no ZooKeeper

Kafka 4.0 removed ZooKeeper support, so KRaft is how Kafka runs now. It also
means one container fewer.

### Outbox table polled by a relay

Options considered:

- Simulator writes to Postgres, then publishes to Kafka itself. These are two
  separate writes with no shared transaction. A crash between them leaves
  Postgres and Kafka disagreeing, with no way to notice.
- Poll the `orders` table by `updated_at`. Only the latest state is visible,
  so an order that goes PAID then SHIPPED between two polls loses its PAID
  event. Rows with identical timestamps are also easy to skip.
- CDC with Debezium, reading Postgres's write-ahead log. This is what
  production systems often use, but it adds Kafka Connect and Debezium to the
  setup and hides the part I want to write myself.
- Outbox: the order change and the event are written in the same transaction,
  and a separate relay publishes the events. **Chosen.**

The cost of the outbox: if the relay crashes after publishing but before
setting `sent_at`, it publishes the same event again on restart. Consumers have
to cope with that (see below).

### Partition key = order_id

Kafka only keeps messages in order inside one partition. Using the order id as
key sends every event of an order to the same partition, so consumers always
see CREATED before PAID before SHIPPED.

The alternative was `customer_id`. It would also keep ordering per customer,
but a few busy customers could overload one partition, and nothing here needs
ordering across different orders of the same customer.

Side effect: one customer's orders are spread across partitions, so the two
analytics instances can update the same `customer_stats` row at the same time.
That is safe because every update is a single `SET x = x + ...` statement,
which Postgres applies under a row lock.

### 3 partitions

Enough to see an uneven split with 2 consumers (2 partitions and 1), and an
idle consumer when scaled to 4.

### At-least-once delivery, offsets committed by hand

`enable.auto.commit=false`. The consumer commits its offset only after the
database transaction has committed. A crash in between means the event is
delivered again on restart. Events are never lost, but can arrive more than
once.

Exactly-once is not attempted. Kafka transactions cover writes to Kafka, not to
Postgres, so for a database sink the usual approach is this one: at-least-once
delivery plus a write that is safe to repeat.

### Analytics skips duplicates, audit keeps them

Analytics keeps totals, so counting an event twice would make them wrong. It
records every event id it has processed and skips ones it has already seen.

Audit only writes a line per event, so a duplicate breaks nothing. It keeps
duplicates on purpose: during the failure tests, the audit log shows that a
duplicate was delivered and `customer_stats` shows it was counted once.

### JSON with a schemaVersion field

Readable with Kafka's console consumer and needs no extra services. Avro with
a Schema Registry would enforce compatibility between producers and consumers;
here the `schemaVersion` field is a manual reminder that the format can
change. Consumers ignore unknown fields, so adding a field does not break them.

### Plain kafka-clients, no Spring

Spring Kafka sets up the producer, the consumer poll loop and offset commits
for you. The point of this project is to write those by hand. Stack: Java 21
(Temurin), Maven multi-module, JDBC with HikariCP, Jackson for JSON.

### One Postgres container, two databases

Keeps the two sides' data separate without running a second container. Easy to
split later if needed.

## Stages

| # | Builds | Exercises |
|---|--------|-----------|
| 0 | This document | |
| 1 | Compose file: Kafka + Postgres, topic creation | Container networking, Kafka listeners, testing with console producer/consumer |
| 2 | Tables + simulator | Transactions, outbox writes |
| 3 | Relay | Producer config: acks, idempotence, serializers, key to partition |
| 4 | Analytics consumer | Poll loop, deserializers, manual commits, duplicate-safe writes |
| 5 | Consumer groups | Partition assignment, rebalancing, separate offsets per group, lag |
| 6 | Failure tests | Crash before commit, redelivery, resuming from committed offset |
| 7 | README | |

## Known limitations

- One broker: no replication and no leader failover.
- One relay instance; running several would need row locking on `outbox`.
- No schema registry.
- No exactly-once processing.
- Traffic is simulated.
- Database credentials are written in plain text in `docker-compose.yml`. Fine for local use only.

## Findings

### Stage 1

- Keys are hashed byte for byte. While testing with the console producer, a
  key typed with leading spaces (`   order-42`) landed in partition 2, while
  `order-42` had gone to partition 0. The relay therefore builds keys only
  from the order UUID read from the database, never from formatted or typed
  text.
- `--from-beginning` only applies to a consumer group with no committed
  offsets. With an existing group, the console consumer resumed from its
  offsets and printed only the 2 messages sent while it was stopped.
- Kafka warns that `.` and `_` collide in metric names. Topic names in this
  project use dots only.
- Kafka 4.3 suggests the new consumer group protocol (KIP-848,
  `group.protocol=consumer`). Classic or new is decided in stage 5.
- The EXTERNAL listener (`localhost:9094`) is configured but not tested yet.
  No client runs outside Docker until stage 3.


### No volumes: data lives as long as the containers

Neither Kafka nor Postgres uses a Docker volume. `docker compose stop` and
`start` keep all data, which is enough for the failure tests.
`docker compose down -v` wipes both at the same time. Resetting only one of
them would leave them disagreeing: the outbox would mark events as sent that
Kafka no longer has.

### Stage 2

- The simulator first ran with no log output at all, only an SLF4J warning
  about falling back to a no-op logger. `mvn dependency:tree` showed that
  HikariCP brings in slf4j-api 1.7.36, and Maven picked it over logback's 2.x
  version because HikariCP is declared first. Logback 1.5 only works with
  slf4j-api 2.x. Fixed by pinning slf4j-api 2.0.17 in the parent pom's
  dependencyManagement, which also applies to indirect dependencies.
- Every Docker build of the simulator re-downloads all Maven dependencies
  (about a minute), because `COPY . .` changes whenever any file changes.
  Acceptable for now.

### Stage 3

- The EXTERNAL listener works: the relay ran from WSL against
  `localhost:9094` and published normally. That confirms clients outside
  Docker get `localhost:9094` back as the address to reconnect to.
- On first start the relay drained a backlog of about 15,800 outbox rows. The
  three partitions ended up with 5,120, 5,482 and 5,276 messages: hashing the
  order id spreads orders evenly.
- Messages in Kafka have their JSON keys in a different order from the
  OrderEvent record, with spaces after colons. The payload column is `jsonb`,
  which stores parsed JSON and writes its own text when read back. The relay
  therefore publishes Postgres's rendering, not the simulator's original
  text. Harmless here, since consumers read fields by name; a `json` or `text`
  column would be needed to keep the exact bytes.

### Stage 4

- First run processed the whole backlog, then kept up with live traffic.
  With the simulator paused, customer_stats matched the orders table
  exactly: 10,890 orders created, 7,733 paid, 1,180,360.24 total paid. The
  analytics side never reads orders_db, so these numbers come only from the
  events in Kafka.
- No duplicates on the first run, as expected: the relay never crashed.
- On shutdown the consumer logs "revoked partitions: [0, 1, 2]" before
  closing: it leaves the group instead of waiting to be timed out.

### Stage 5

- Scaling the analytics group with `docker compose up --scale`:
  1 consumer had [0, 1, 2]; with 2 consumers the split was [0, 1] and [2];
  with 4 consumers one got `assigned partitions: []` and sat idle. A group
  cannot use more consumers than the topic has partitions.
- Every change triggered a full rebalance: each member first revoked all its
  partitions, then received a new assignment. Existing members did not keep
  their old partitions (the consumer holding [2] ended up with nothing).
- Lag stayed at 0 through every rebalance. Offsets belong to the group, not
  to a consumer, so a new owner continues from the group's committed offset.
- Decision on the open stage 1 question: the classic group protocol is kept.
  Its revoke-everything behaviour is visible in the logs and is what most
  documentation describes. The KIP-848 protocol moves assignment to the
  broker and only moves the partitions that need to move.
- The audit consumer, in its own group, started with no committed offsets and
  copied the whole topic from offset 0: 76,544 lines against 76,548 messages
  a few seconds later.
- With the audit consumer stopped for 30 seconds, the analytics group stayed
  at lag 0 while the audit group's lag grew to 38. Its committed offsets
  remained visible with no active member. On restart the first batch was 59
  lines, then it was back to live traffic.

### Stage 6

- Relay crash after Kafka confirmed a batch of 8, before sent_at was written:
  on restart all 8 events were published again at new offsets (for example
  event 9073bb76 at offsets 32959 and 32961 of partition 2). The audit log
  has both copies; the analytics consumer logged 8 "skipped duplicate" lines.
- Analytics crash after writing 10 records, before the offset commit: the
  batch was redelivered. The first batch after restart had 44 records, 34
  applied and 10 duplicates.
- A hard crash froze the analytics group for about 45 seconds. The process
  was halted without leaving the group, so the dead member kept its
  partitions until session.timeout.ms (45 s by default) expired: crash at
  19:14:59, new assignment at 19:15:44. A clean shutdown leaves the group
  immediately. A lower timeout would detect crashes sooner but could evict a
  consumer that is only slow, for example during a long GC pause. Kept the
  default.
- The first totals comparison after that crash showed 16 orders missing. It
  ran during the freeze. With lag confirmed at 0 on every partition, the
  totals matched exactly: 39,445 orders, 27,900 paid, 4,254,270.92 total.
  Comparing a consumer's totals only means something at lag 0.
