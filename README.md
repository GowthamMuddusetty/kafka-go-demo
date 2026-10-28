# Kafka + Golang Learning Project

A beginner-friendly project demonstrating how to build event-driven services using **Apache Kafka and Go (Golang)**.

The project simulates an order-processing system with an Order Service, Email Service, and Notification Service.

## Architecture

```text
                 ┌──────────────────┐
                 │  Order Service   │
                 │  Kafka Producer  │
                 └────────┬─────────┘
                          │
                          ▼
                 ┌──────────────────┐
                 │   order-events   │
                 │   Kafka Topic    │
                 └────────┬─────────┘
                          │
                ┌─────────┴──────────┐
                ▼                    ▼
       ┌────────────────┐   ┌────────────────────┐
       │ Email Service  │   │ Notification       │
       │ Consumer Group │   │ Service            │
       └────────┬───────┘   └──────────┬─────────┘
                │                      │
         Retries exhausted       Retries exhausted
                │                      │
                ▼                      ▼
       ┌─────────────────────┐ ┌────────────────────────┐
       │ email-failed-events │ │ notification-failed-   │
       │      Email DLQ      │ │ events (Notification   │
       └─────────────────────┘ │ DLQ)                   │
                               └────────────────────────┘
```

## Project Structure

```text
kafka-go-demo/
├── docker-compose.yml
├── go.mod
├── shared/
│   └── order_event.go
├── order-service/
│   └── main.go
├── email-service/
│   └── main.go
└── notification-service/
    └── main.go
```

## Technologies

- Go (Golang)
- Apache Kafka
- Docker Compose
- `github.com/segmentio/kafka-go`
- JSON for event serialization

## Kafka Topics

| Topic | Purpose |
|---|---|
| `order-events` | Carries order events from the producer to consumers |
| `email-failed-events` | Stores messages that fail email processing or contain invalid JSON |
| `notification-failed-events` | Stores messages that fail notification processing or contain invalid JSON |

All three topics are configured with three partitions and replication factor one for local development.

## Concepts Demonstrated

- **Producer and Consumer:** Publish and process Kafka events.
- **Topics and Partitions:** Organize events and enable parallel processing.
- **Message Keys:** Use order IDs to route related events consistently.
- **Consumer Groups:** Allow service instances to share partition assignments.
- **Consumer Offsets:** Track consumer progress.
- **Manual Offset Commits:** Commit messages after successful processing or successful DLQ publishing.
- **Retries:** Retry failed email and notification operations up to three times.
- **Exponential Backoff:** Wait one second and then two seconds between retries.
- **Dead Letter Queues (DLQs):** Preserve failed messages for investigation.
- **Graceful Shutdown:** Handle termination signals and close Kafka resources.
- **Basic Idempotency:** Use an in-memory Go map to avoid repeated email sends within one service run.

## Prerequisites

Install:

- Go
- Docker Desktop
- Git

## Run the Project

### 1. Start Kafka and create topics

From the project root:

```bash
docker compose up -d
```

List the topics:

```bash
docker exec kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Expected topics include:

```text
email-failed-events
notification-failed-events
order-events
```

### 2. Start the Email Service

Open a terminal:

```bash
go run ./email-service
```

### 3. Start the Notification Service

Open another terminal:

```bash
go run ./notification-service
```

### 4. Publish order events

Open another terminal:

```bash
go run ./order-service
```

The producer publishes sample orders, including `order-104`, which simulates failures in both services.

## Testing Scenarios

### 1. Test Producer and Consumers

Start Kafka, both consumer services, and then the producer.

**Expected result:** Both services independently receive and process events from `order-events`.

### 2. Test Email Service Retries

The Email Service simulates a failure for `order-104`.

**Expected result:**

1. Attempt 1 fails.
2. Wait 1 second.
3. Attempt 2 fails.
4. Wait 2 seconds.
5. Attempt 3 fails.
6. The event is published to `email-failed-events`.

### 3. Test Notification Service Retries

The Notification Service simulates a failure for `order-104`.

**Expected result:**

1. Attempt 1 fails.
2. Wait 1 second.
3. Attempt 2 fails.
4. Wait 2 seconds.
5. Attempt 3 fails.
6. The event is published to `notification-failed-events`.

### 4. Understand Exponential Backoff

Both services increase the delay between retry attempts.

| Retry interval | Delay |
|---|---:|
| After attempt 1 | 1 second |
| After attempt 2 | 2 seconds |
| After attempt 3 | No further wait |

The Notification Service uses `select` with `time.After` and `ctx.Done()` so that shutdown can interrupt the retry wait.

### 5. Test the Email DLQ

After the Email Service exhausts its retries, inspect the failed events:

```bash
docker exec -it kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic email-failed-events --from-beginning
```

**Expected result:** Failed email events appear in the output.

### 6. Test the Notification DLQ

After the Notification Service exhausts its retries, inspect the failed events:

```bash
docker exec -it kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic notification-failed-events --from-beginning
```

**Expected result:** The original event for `order-104` appears in the Notification DLQ.

The notification service also sends invalid JSON messages to this DLQ. Both DLQs preserve the original message value and key, along with an `original-topic` header.

### 7. Test Graceful Shutdown

Start either consumer and press `Ctrl+C`.

**Expected result:** The service receives the shutdown signal and attempts to close its Kafka resources cleanly. During a retry wait, the Notification Service can stop without waiting for the full delay.

### 8. Test Consumer Groups

The services use different group IDs:

- `email-service-group`
- `notification-service-group`

**Expected result:** Both groups independently receive the events. Their committed offsets are tracked separately.

### 9. Test Multiple Consumer Instances

Start multiple Email Service instances using the same `email-service-group`.

**Expected result:** Kafka distributes partitions among active group members. With three partitions, up to three consumers can own partitions simultaneously within that group.

Stop one consumer and observe the group rebalance.

### 10. Understand Message Keys and Partitions

The producer uses the order ID as the message key and `kafka.Hash{}` as the balancer.

**Expected result:** The same key routes to the same partition while the partitioning configuration remains stable. Different keys can map to the same partition.

### 11. Inspect Kafka Topics and Partitions

List topics:

```bash
docker exec kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Describe the order topic:

```bash
docker exec kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic order-events --bootstrap-server localhost:9092
```

Describe the Notification DLQ:

```bash
docker exec kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-topics.sh --describe --topic notification-failed-events --bootstrap-server localhost:9092
```

**Expected result:** Inspect the topic partition counts and partition leaders.

### 12. Inspect Consumer Offsets and Lag

For the Email Service:

```bash
docker exec kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group email-service-group
```

For the Notification Service:

```bash
docker exec kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notification-service-group
```

Understand these columns:

- `CURRENT-OFFSET`: the committed consumer position.
- `LOG-END-OFFSET`: the end of the partition log.
- `LAG`: the difference between the log end and committed position.

A lag of zero generally means the group has caught up with the available messages.

### 13. Test Duplicate Order IDs

Run the producer multiple times or publish the same order ID again.

**Expected result:** Kafka stores each publication as a separate record. The Email Service's in-memory map skips an order ID already successfully processed by that same running instance.

Restarting the Email Service clears the map, so this is not durable deduplication.

### 14. Test Invalid JSON

Publish malformed JSON to `order-events` using a Kafka console producer:

```bash
docker exec -it kafka-go-demo-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic order-events
```

Type an invalid JSON message:

```text
{invalid-json
```

**Expected result:**

- The Email Service logs the JSON error and attempts to publish the message to `email-failed-events`.
- The Notification Service logs the JSON error and attempts to publish the message to `notification-failed-events`.

If DLQ publishing fails, the affected message is not committed by that service.

## Production Readiness

This repository is a **learning and demonstration project, not a production-ready system**.

Areas requiring further work include:

- **Persistent idempotency:** Replace the in-memory map with durable storage or provider-supported idempotency keys.
- **Offset management:** Handle commit failures and invalid messages carefully because messages may be delivered again.
- **DLQ reliability:** Handle duplicate DLQ publications, retries, monitoring, and recovery.
- **Partition ordering:** Handle offset commits carefully to avoid skipping earlier unprocessed messages when a later message is committed.
- **Real integrations:** Replace simulated email and notification functions with actual providers.
- **Observability:** Add structured logs, metrics, alerts, and health checks.
- **Security:** Configure authentication, TLS, and appropriate Kafka access controls.
- **Resilience:** Add timeouts, retry policies, and integration tests.
- **Deployment:** Configure multiple brokers and replication for high availability.

## Learning Goal

The goal is to understand Kafka fundamentals through a small, runnable Go project before moving on to more advanced production patterns.

Contributions, suggestions, and improvements are welcome.
