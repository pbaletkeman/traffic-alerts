# Real-Time Traffic Monitoring & Alerting System

(Kafka + Java + Kafka Streams + Kafka Connect + Security + Observability)

## 1. Project Overview

A distributed, real-time traffic analytics system built to demonstrate end-to-end Kafka developer proficiency. It ingests live sensor data, processes it using Kafka Streams, applies alerting logic, integrates with Kafka Connect, and exposes full observability, testing, and security patterns required for the Confluent Certified Developer exam.

**[Project Epics](project-epics.md)**

This version covers all six exam sections:

| Exam Section              |  %   | Project Coverage                                               |
| ---                       | ---: | ---                                                            |
| Kafka Fundamentals        | 23%  | Topics, partitions, offsets, retention, replication, CLI tools |
| Application Development   | 28%  | Producer, consumer, serialization, error handling, Admin API   |
| Kafka Streams             | 12%  | State stores, windowing, exactly-once, KTables                 |
| Kafka Connect             | 15%  | Source + sink connectors, CDC concepts                         |
| Application Testing       | 8%   | Embedded Kafka, Testcontainers, topology tests                 |
| Application Observability | 13%  | JMX metrics, Prometheus, Grafana                               |

## 2. High-level architecture

1. **Java Kafka Producer**
   - Publishes raw sensor events to `traffic_raw`.
   - Uses:
     - Key hashing for partitioning
     - Compression (lz4/snappy)
     - Batch + linger tuning
     - SSL/SASL security
     - Avro serialization with Schema Registry

2. **Kafka Streams Processor (Java)**
   - Reads from `traffic_raw`.
   - Performs all stream processing:
     - Time-windowed aggregations (tumbling + sliding)
     - State stores for congestion history per road segment
     - Congestion score calculation
     - Alert emission when thresholds exceeded
   - Writes enriched events to `traffic_processed`.
   - Emits alerts to `traffic_alerts`.
   - Uses:
     - Exactly-once semantics (`exactly_once_v2`)
     - KTables + state stores
     - Dead-letter topic routing

3. **Kafka Connect Layer**
   - Source connector: ingest static road metadata from PostgreSQL → Kafka.
   - Sink connector: store alerts into PostgreSQL.

4. **Dashboard (Spring Boot + Thymeleaf)**
   - Simple server-rendered dashboard showing:
     - Live traffic flow (polling-based)
     - Current alerts
     - Basic Kafka metrics

5. **Observability**
   - Prometheus + Grafana
   - JMX metrics for Kafka clients
   - Alerting rules for system health

## 3. Kafka topics

| Topic               | Purpose              | Partitions  | Notes                 |
| -------             |---------             | ----------: | -------               |
| `traffic_raw`       | Raw sensor events    | 6           | Keyed by road_segment |
| `traffic_processed` | Enriched analytics   | 6           | Compacted optional    |
| `traffic_alerts`    | Alert notifications  | 3           | Short retention       |
| `road_metadata`     | Kafka Connect source | 1           | Static reference data |

Includes:

- Replication factor: 3
- Retention policies
- Min ISR configuration
- Compression: lz4

## 4. Data model

Supports JSON and Avro via Schema Registry.

### Raw sensor event (JSON)

```json
{
  "sensor_id": "S123",
  "road_segment": "R45",
  "vehicle_count": 12,
  "avg_speed": 48.5,
  "timestamp": "2026-08-15T18:26:00Z"
}
```

### Processed event (JSON)

```json
{
  "road_segment": "R45",
  "window_avg_speed": 42.1,
  "congestion_score": 0.78,
  "window_start": "2026-08-15T18:26:00Z",
  "window_end": "2026-08-15T18:26:10Z"
}
```

### Alert event (JSON)

```json
{
  "road_segment": "R45",
  "alert_type": "CONGESTION",
  "severity": "HIGH",
  "timestamp": "2026-08-15T18:26:05Z"
}
```

## 5. Technologies used

### Core infrastructure

- Apache Kafka (topics, partitions, offsets, retention)
- KRaft mode (no Zookeeper)
- Schema Registry
- Kafka Connect

### Processing layer

- Kafka Streams (Java) — all stream processing, windowing, state stores

### Java components

- Kafka Java client (Apache or Confluent)
- Spring Boot (dashboard REST + Thymeleaf)
- JUnit 5 + Testcontainers + Embedded Kafka

### Storage

- PostgreSQL (sink connector)

### Security

- SSL encryption
- SASL/SCRAM authentication
- ACLs for producers/consumers

### Observability

- Prometheus
- Grafana
- JMX metrics

### Deployment & ops

- Docker / Docker Compose
- GitHub Actions CI/CD

## 6. System workflow

### Step 1 — Data ingestion

Java producer → Kafka (`traffic_raw`)

Includes:

- Keying strategy
- Compression
- Batching
- Retryable vs non-retryable error handling

### Step 2 — Kafka Streams processing

Kafka Streams reads `traffic_raw` → applies:

- Time-windowed aggregation (10s window, 5s slide)
- State store tracking per road segment
- Congestion score calculation
- Alert emission when threshold exceeded

Outputs:

- Enriched events → `traffic_processed`
- Alerts → `traffic_alerts`

### Step 3 — Kafka Connect

- Source: road metadata → Kafka
- Sink: alerts → PostgreSQL

### Step 4 — Visualization

Spring Boot dashboard polls REST API and displays:

- Current traffic conditions
- Active alerts
- Basic metrics

## 7. Kafka Streams logic

### Topology

- KStream from `traffic_raw`
- GroupBy road segment
- Time-windowed aggregation
- State store for historical congestion
- Emit alerts when thresholds exceeded
- Branch to `traffic_processed` and `traffic_alerts`

### Exactly-once semantics

`processing.guarantee=exactly_once_v2`

### Error handling

- Dead-letter topic: `traffic_dlq`
- Retriable vs non-retriable classification

## 8. Kafka Connect integration

### Source connector (PostgreSQL → Kafka)

- Road metadata
- Enrichment for Streams processing

### Sink connector (Kafka → PostgreSQL)

- Persist alerts
- Dashboard reads from DB

## 9. Testing

Includes:

- Embedded Kafka for unit tests
- Testcontainers for integration tests
- Producer/consumer load tests
- Streams topology tests (TopologyTestDriver)
- Schema validation tests

## 10. Observability

- JMX metrics for producers/consumers
- Streams metrics (latency, throughput, commit rate)
- Prometheus exporters
- Grafana dashboards
- Prometheus alerting rules

## 11. Directory structure

```text
traffic-system/
├── producer/
│   └── Producer.java
├── streams/
│   └── TrafficStreamsApp.java
├── connect/
│   ├── source-config.json
│   └── sink-config.json
├── dashboard/
│   └── DashboardApplication.java
├── observability/
│   ├── prometheus.yml
│   └── grafana_dashboards/
├── tests/
│   ├── EmbeddedKafkaTests.java
│   └── StreamsTopologyTests.java
├── config/
│   └── kafka_topics.json
└── docker-compose.yml
```

## 12. Summary

This project demonstrates full Kafka developer competency, covering all six Confluent Certified Developer exam sections:

- **Kafka Fundamentals** — topics, partitions, offsets, retention, replication, CLI tools
- **Application Development** — producer, consumer, serialization, error handling, Admin API
- **Kafka Streams** — state stores, windowing, exactly-once semantics, KTables
- **Kafka Connect** — source and sink connectors, CDC concepts
- **Application Testing** — Embedded Kafka, Testcontainers, topology tests
- **Application Observability** — JMX metrics, Prometheus, Grafana

It is suitable as a portfolio flagship project for:

- Confluent Certified Developer for Apache Kafka
- Backend engineering roles
- Data engineering roles
- Streaming architecture roles
