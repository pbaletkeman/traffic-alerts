# Real-Time Traffic Monitoring & Alerting System — User Stories with Definitions of Done

- [Real-Time Traffic Monitoring \& Alerting System — User Stories with Definitions of Done](#real-time-traffic-monitoring--alerting-system--user-stories-with-definitions-of-done)
  - [EPIC 1 — Kafka Data Ingestion (Java Producer)](#epic-1--kafka-data-ingestion-java-producer)
    - [Story 1.1 — Implement Java Kafka Producer](#story-11--implement-java-kafka-producer)
    - [Story 1.2 — Publish Raw Events to `traffic_raw` Topic](#story-12--publish-raw-events-to-traffic_raw-topic)
    - [Story 1.3 — Add Config File for Kafka Producer](#story-13--add-config-file-for-kafka-producer)
  - [EPIC 2 — Spark Structured Streaming Processor (Java)](#epic-2--spark-structured-streaming-processor-java)
    - [Story 2.1 — Create Spark Streaming Job Skeleton](#story-21--create-spark-streaming-job-skeleton)
    - [Story 2.2 — Read Raw Events from Kafka](#story-22--read-raw-events-from-kafka)
    - [Story 2.3 — Implement Sliding Window Aggregation](#story-23--implement-sliding-window-aggregation)
    - [Story 2.4 — Implement Congestion Score Formula](#story-24--implement-congestion-score-formula)
    - [Story 2.5 — Write Processed Events to `traffic_processed` Topic](#story-25--write-processed-events-to-traffic_processed-topic)
  - [EPIC 3 — Java Alert Engine](#epic-3--java-alert-engine)
    - [Story 3.1 — Implement Kafka Consumer for Processed Events](#story-31--implement-kafka-consumer-for-processed-events)
    - [Story 3.2 — Implement Alert Rule Logic](#story-32--implement-alert-rule-logic)
    - [Story 3.3 — Publish Alerts to `traffic_alerts` Topic](#story-33--publish-alerts-to-traffic_alerts-topic)
  - [EPIC 4 — Dashboard (Optional)](#epic-4--dashboard-optional)
    - [Story 4.1 — Create Dashboard Application Skeleton](#story-41--create-dashboard-application-skeleton)
    - [Story 4.2 — Implement Live Traffic View](#story-42--implement-live-traffic-view)
    - [Story 4.3 — Implement Alerts View](#story-43--implement-alerts-view)
    - [Story 4.4 — Add Heatmap Visualization](#story-44--add-heatmap-visualization)
  - [EPIC 5 — Infrastructure \& Deployment](#epic-5--infrastructure--deployment)
    - [Story 5.1 — Create Docker Compose for Full System](#story-51--create-docker-compose-for-full-system)
    - [Story 5.2 — Add Monitoring (Prometheus + Grafana)](#story-52--add-monitoring-prometheus--grafana)
  - [EPIC 6 — Optional Enhancements](#epic-6--optional-enhancements)
    - [Story 6.1 — Add ML-Based Anomaly Detection](#story-61--add-ml-based-anomaly-detection)
    - [Story 6.2 — Add Delta Lake Persistence](#story-62--add-delta-lake-persistence)
    - [Story 6.3 — Scale Kafka \& Spark](#story-63--scale-kafka--spark)

## EPIC 1 — Kafka Data Ingestion (Java Producer)

### Story 1.1 — Implement Java Kafka Producer

**Definition of Done:**

- Java app produces JSON events matching the Raw sensor event schema: "sensor_id, road_segment, vehicle_count, avg_speed, timestamp".
- Producer connects to Kafka broker.
- Events logged locally before sending.
- Optional: code containerized.

### Story 1.2 — Publish Raw Events to `traffic_raw` Topic

**Definition of Done:**

- Producer sends events to Kafka topic "traffic_raw".
- Topic exists or auto-creates.
- Kafka CLI verifies messages.
- At least 100 sample events published.

### Story 1.3 — Add Config File for Kafka Producer

**Definition of Done:**

- `config/kafka_topics.json` contains producer topic config.
- Producer loads config at runtime.
- Changing config does not require code changes.

---

## EPIC 2 — Spark Structured Streaming Processor (Java)

### Story 2.1 — Create Spark Streaming Job Skeleton

**Definition of Done:**

- `StreamingJob.java` created.
- Spark session initializes.
- App runs locally or in cluster mode.

### Story 2.2 — Read Raw Events from Kafka

**Definition of Done:**

- Spark reads from "traffic_raw".
- JSON parsed into typed dataset.
- Invalid JSON logged and skipped.

### Story 2.3 — Implement Sliding Window Aggregation

**Definition of Done:**

- Window duration: 10 seconds; Slide interval: 5 seconds.
- Computes `window_avg_speed`, `vehicle density`, `congestion_score`.
- Watermarking applied.
- Unit tests validate window logic.

### Story 2.4 — Implement Congestion Score Formula

**Definition of Done:**

- `congestion_score = (max_speed - window_avg_speed) / max_speed`
- Max speed configurable.
- Score validated with sample inputs.

### Story 2.5 — Write Processed Events to `traffic_processed` Topic

**Definition of Done:**

- Output JSON matches schema: "road_segment, window_avg_speed, congestion_score, timestamp".
- Kafka consumer verifies messages.
- End-to-end test validates ingestion → processing → output.

---

## EPIC 3 — Java Alert Engine

### Story 3.1 — Implement Kafka Consumer for Processed Events

**Definition of Done:**

- Java consumer reads from "traffic_processed".
- JSON parsed into POJO.
- Consumer handles retries and failures.

### Story 3.2 — Implement Alert Rule Logic

**Definition of Done:**

- Rule engine evaluates congestion threshold.
- Threshold configurable.
- Alerts include: "road_segment, alert_type, severity, timestamp".

### Story 3.3 — Publish Alerts to `traffic_alerts` Topic

**Definition of Done:**

- Alerts published to Kafka topic "traffic_alerts".
- Kafka CLI verifies alert messages.
- At least 20 alerts generated during test run.

---

## EPIC 4 — Dashboard (Optional)

### Story 4.1 — Create Dashboard Application Skeleton

**Definition of Done:**

- Spring Boot or React app created.
- Basic homepage loads.

### Story 4.2 — Implement Live Traffic View

**Definition of Done:**

- Dashboard subscribes to "traffic_processed".
- Displays road segment, avg speed, congestion score.
- Auto-refresh every 5 seconds.

### Story 4.3 — Implement Alerts View

**Definition of Done:**

- Dashboard subscribes to "traffic_alerts".
- Alerts displayed in real-time.
- Severity color-coded.

### Story 4.4 — Add Heatmap Visualization

**Definition of Done:**

- Road segments displayed on map/grid.
- Congestion score mapped to color intensity.
- Updates based on streaming data.

---

## EPIC 5 — Infrastructure & Deployment

### Story 5.1 — Create Docker Compose for Full System

**Definition of Done:**

- `docker-compose.yml` includes Kafka, Zookeeper/KRaft, Producer, Spark job, Alert engine, Dashboard.
- `docker compose up` starts all services.

### Story 5.2 — Add Monitoring (Prometheus + Grafana)

**Definition of Done:**

- Prometheus scrapes producer, Spark, and alert engine metrics.
- Grafana dashboard created with throughput, latency, alert frequency.

---

## EPIC 6 — Optional Enhancements

### Story 6.1 — Add ML-Based Anomaly Detection

**Definition of Done:**

- MLlib model integrated into Spark job.
- Model predicts anomalies based on speed/vehicle count.
- Anomalies published to `traffic_anomalies`.

### Story 6.2 — Add Delta Lake Persistence

**Definition of Done:**

- Raw + processed data stored in Delta Lake.
- Time-travel queries enabled.
- Historical dashboard created.

### Story 6.3 — Scale Kafka & Spark

**Definition of Done:**

- Kafka partitions increased.
- Spark cluster runs with multiple executors.
- Load test validates horizontal scaling.
