# Real-Time Traffic Monitoring & Alerting System

(Kafka + Java + Spark Structured Streaming + Kafka Streams + Kafka Connect + Security + Observability)

## 1. Project Overview

A distributed, real-time traffic analytics system built to demonstrate end‑to‑end Kafka developer proficiency. It ingests live sensor data, processes it using Spark Structured Streaming and Kafka Streams, applies alerting logic in Java, integrates with Kafka Connect, and exposes full observability, testing, and security patterns required for the Confluent Certified Developer exam.

**[Project Epics](project-epics.md)**

This version covers:

- Kafka fundamentals (topics, partitions, offsets, retention, replication)
- Kafka client development (producers, consumers, batching, compression)
- Kafka Streams (state stores, windowing, exactly-once semantics)
- Kafka Connect (source + sink connectors)
- Serialization (Avro, JSON, Protobuf)
- Security (SSL, SASL, ACLs)
- Application testing (mocking, embedded Kafka)
- Observability (metrics, logging, monitoring)

## 2. High-level architecture

1. **Java Kafka Producer**
   - Publishes raw sensor events to `traffic_raw`.
   - Uses:
     - Key hashing for partitioning
     - Compression (lz4/snappy)
     - Batch + linger tuning
     - SSL/SASL security

2. **Spark Structured Streaming Processor (Java)**
   - Reads from `traffic_raw`.
   - Performs distributed analytics:
     - Sliding windows
     - Watermarking
     - Aggregations
     - ML-based anomaly detection
   - Writes enriched events to `traffic_processed`.

3. **Kafka Streams Processor (Java)**
   - Consumes `traffic_processed`.
   - Maintains state stores for:
     - Road congestion trends
     - Speed anomaly detection
   - Emits alerts to `traffic_alerts`.
   - Uses:
     - Exactly-once semantics
     - KTables + state stores
     - Stream-stream joins (optional)

4. **Java Alert Engine**
   - Consumes `traffic_alerts`.
   - Applies business rules.
   - Sends notifications to:
     - Kafka Connect sink (PostgreSQL)
     - Dashboard API

5. **Kafka Connect Layer**
   - Source connector: ingest static road metadata from PostgreSQL → Kafka.
   - Sink connector: store alerts into PostgreSQL or MongoDB.

6. **Dashboard (Spring Boot / React)**
   - Visualizes:
     - Live traffic flow
     - Congestion zones
     - Alerts
     - Kafka metrics

7. **Observability**
   - Prometheus + Grafana
   - JMX metrics for Kafka clients
   - Spark metrics sink

## 3. Kafka topics

| Topic               | Purpose              | Partitions  | Notes                 |
| -------             |---------             | ----------- | -------               |
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

Supports JSON, Avro, and Protobuf via Schema Registry.

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
  "timestamp": "2026-08-15T18:26:05Z"
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
- Zookeeper / KRaft
- Schema Registry
- Kafka Connect

### Processing layer

- Spark Structured Streaming (Java API)
- Kafka Streams (Java)
- Spark SQL
- Spark MLlib (optional)

### Java components

- Kafka Java client (Apache or Confluent)
- Spring Boot / Jakarta EE
- JUnit + Testcontainers + Embedded Kafka

### Storage

- PostgreSQL (sink connector)
- MongoDB (optional)
- Delta Lake (optional)

### Security

- SSL encryption
- SASL/SCRAM authentication
- ACLs for producers/consumers

### Observability

- Prometheus
- Grafana
- JMX metrics
- Kafka client metrics

### Deployment & ops

- Docker / Docker Compose
- Kubernetes (optional)
- CI/CD with GitHub Actions

## 6. System workflow

### Step 1 — Data ingestion

Java producer → Kafka (`traffic_raw`)

Includes:

- Keying strategy
- Compression
- Batching
- Retryable vs non-retryable error handling

### Step 2 — Distributed stream processing (Spark)

Spark reads `traffic_raw` → applies:

- Sliding windows
- Watermarking
- Aggregations
- ML anomaly detection

Outputs → `traffic_processed`.

### Step 3 — Kafka Streams processing

Kafka Streams consumes `traffic_processed` → applies:

- State store tracking
- Exactly-once semantics
- Stream joins (optional)

Outputs → `traffic_alerts`.

### Step 4 — Alerting engine

Java consumer → evaluates:

- Congestion thresholds
- Speed drops
- Traffic spikes

Outputs → Kafka Connect sink + dashboard.

### Step 5 — Kafka Connect

- Source: road metadata → Kafka
- Sink: alerts → PostgreSQL

### Step 6 — Visualization

Dashboard displays:

- Live traffic
- Heatmaps
- Alerts
- Kafka metrics

## 7. Spark streaming logic (conceptual)

### Sliding window aggregation

- Window: 10 seconds
- Slide: 5 seconds
- Compute:
  - Average speed
  - Vehicle density
  - Congestion score

### Congestion score formula

[
text{congestion_score} = frac{text{max_speed} - text{window_avg_speed}}{text{max_speed}}
]

### Alert rule (Java-style pseudocode)

```java
if (congestionScore > 0.7) {
    emitAlert("CONGESTION", "HIGH");
}
```

## 8. Kafka Streams logic

### Topology

- KStream from `traffic_processed`
- GroupBy road segment
- Time-windowed aggregation
- State store for historical congestion
- Emit alerts when thresholds exceeded

### Exactly-once semantics

`processing.guarantee=exactly_once_v2`

### Error handling

- Dead-letter topic: `traffic_dlq`
- Retriable vs non-retriable classification

## 9. Kafka Connect integration

### Source connector (PostgreSQL → Kafka)

- Road metadata
- Enrichment for Spark + Streams

### Sink connector (Kafka → PostgreSQL)

- Persist alerts
- Dashboard reads from DB

## 10. Testing

Includes:

- Embedded Kafka for unit tests
- Testcontainers for integration tests
- Producer/consumer load tests
- Streams topology tests
- Schema validation tests

## 11. Observability

- JMX metrics for producers/consumers
- Streams metrics (latency, throughput, commit rate)
- Spark metrics sink
- Prometheus exporters
- Grafana dashboards

## 12. Directory structure

```text
traffic-system/
│
├── producer/
│   └── Producer.java
│
├── spark/
│   ├── StreamingJob.java
│   └── ml_models/
│
├── streams/
│   └── TrafficStreamsApp.java
│
├── alert_engine/
│   └── AlertConsumer.java
│
├── connect/
│   ├── source-config.json
│   └── sink-config.json
│
├── dashboard/
│   └── DashboardApplication.java
│
├── observability/
│   ├── prometheus.yml
│   └── grafana_dashboards/
│
├── tests/
│   ├── EmbeddedKafkaTests.java
│   └── StreamsTopologyTests.java
│
├── config/
│   └── kafka_topics.json
│
└── docker-compose.yml
```

## 13. Summary

This enhanced project now demonstrates full Kafka developer competency, covering:

- Kafka fundamentals
- Kafka client development
- Kafka Streams
- Kafka Connect
- Serialization (Avro/JSON/Protobuf)
- Security (SSL/SASL/ACLs)
- Testing (embedded Kafka, topology tests)
- Observability (Prometheus/Grafana)
- Distributed processing (Spark)
- Real-time alerting (Java)

It is now suitable as a portfolio flagship project for:

- Confluent Certified Developer for Apache Kafka®
- Backend engineering roles
- Data engineering roles
- Streaming architecture roles
