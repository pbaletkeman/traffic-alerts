# Real-Time Traffic Monitoring & Alerting System — Epics

## [Project Overview](project-overview.md)

- [Real-Time Traffic Monitoring \& Alerting System — Epics](#real-time-traffic-monitoring--alerting-system--epics)
  - [Project Overview](#project-overview)
  - [Release Milestones](#release-milestones)
    - [Milestone 1: Foundation (Sprint 1-2)](#milestone-1-foundation-sprint-1-2)
    - [Milestone 2: Core Pipeline (Sprint 3-5)](#milestone-2-core-pipeline-sprint-3-5)
    - [Milestone 3: Integration (Sprint 6-7)](#milestone-3-integration-sprint-6-7)
    - [Milestone 4: Polish (Sprint 8-9)](#milestone-4-polish-sprint-8-9)
  - [Success Criteria](#success-criteria)
  - [Epic 1: Kafka Infrastructure \& Topic Management](#epic-1-kafka-infrastructure--topic-management)
    - [Epic 1: Description](#epic-1-description)
    - [Epic 1: User Stories](#epic-1-user-stories)
    - [Epic 1: Tasks](#epic-1-tasks)
    - [Epic 1: Definition of Done](#epic-1-definition-of-done)
  - [Epic 2: Java Kafka Producer (Data Ingestion)](#epic-2-java-kafka-producer-data-ingestion)
    - [Epic 2: Description](#epic-2-description)
    - [Epic 2: User Stories](#epic-2-user-stories)
    - [Epic 2: Tasks](#epic-2-tasks)
    - [Epic 2: Definition of Done](#epic-2-definition-of-done)
  - [Epic 3: Kafka Streams Processor (Stream Processing + Alerting)](#epic-3-kafka-streams-processor-stream-processing--alerting)
    - [Epic 3: Description](#epic-3-description)
    - [Epic 3: User Stories](#epic-3-user-stories)
    - [Epic 3: Tasks](#epic-3-tasks)
    - [Epic 3: Definition of Done](#epic-3-definition-of-done)
  - [Epic 4: Kafka Connect Integration](#epic-4-kafka-connect-integration)
    - [Epic 4: Description](#epic-4-description)
    - [Epic 4: User Stories](#epic-4-user-stories)
    - [Epic 4: Tasks](#epic-4-tasks)
    - [Epic 4: Definition of Done](#epic-4-definition-of-done)
  - [Epic 5: Dashboard (Spring Boot)](#epic-5-dashboard-spring-boot)
    - [Epic 5: Description](#epic-5-description)
    - [Epic 5: User Stories](#epic-5-user-stories)
    - [Epic 5: Tasks](#epic-5-tasks)
    - [Epic 5: Definition of Done](#epic-5-definition-of-done)
  - [Epic 6: Security Implementation](#epic-6-security-implementation)
    - [Epic 6: Description](#epic-6-description)
    - [Epic 6: User Stories](#epic-6-user-stories)
    - [Epic 6: Tasks](#epic-6-tasks)
    - [Epic 6: Definition of Done](#epic-6-definition-of-done)
  - [Epic 7: Observability \& Monitoring](#epic-7-observability--monitoring)
    - [Epic 7: Description](#epic-7-description)
    - [Epic 7: User Stories](#epic-7-user-stories)
    - [Epic 7: Tasks](#epic-7-tasks)
    - [Epic 7: Definition of Done](#epic-7-definition-of-done)
  - [Epic 8: Testing, Deployment \& Documentation](#epic-8-testing-deployment--documentation)
    - [Epic 8: Description](#epic-8-description)
    - [Epic 8: User Stories](#epic-8-user-stories)
    - [Epic 8: Tasks](#epic-8-tasks)
    - [Epic 8: Definition of Done](#epic-8-definition-of-done)
  - [Epic Summary](#epic-summary)
  - [Certification Topic Coverage Matrix](#certification-topic-coverage-matrix)

---

## Release Milestones

### Milestone 1: Foundation (Sprint 1-2)

- [ ] Kafka infrastructure operational
- [ ] Topics created and configured
- [ ] Schema Registry running

### Milestone 2: Core Pipeline (Sprint 3-5)

- [ ] Producer publishing to Kafka
- [ ] Streams processor generating enriched events and alerts
- [ ] Security implemented

### Milestone 3: Integration (Sprint 6-7)

- [ ] Kafka Connect connectors running
- [ ] Dashboard displaying data
- [ ] Testing framework complete

### Milestone 4: Polish (Sprint 8-9)

- [ ] Observability stack operational
- [ ] CI/CD pipeline running
- [ ] Documentation complete

---

## Success Criteria

1. **Functional:** All components communicate via Kafka with correct data flow
2. **Performance:** System handles 10,000+ events/second end-to-end
3. **Reliability:** Zero data loss under normal operation
4. **Security:** All communication encrypted and authenticated
5. **Observability:** All metrics visible in Grafana dashboards
6. **Testing:** 80%+ unit test coverage, all integration tests passing
7. **Documentation:** New developer can onboard in under 30 minutes
8. **Certification:** Demonstrates all Confluent Certified Developer exam topics

---

## Epic 1: Kafka Infrastructure & Topic Management

**Epic Owner:** Platform Team
**Priority:** Critical
**Target Release:** Sprint 1-2
**Exam Coverage:** Section 1 — Kafka Fundamentals (23%)

### Epic 1: Description

Set up the Kafka infrastructure using KRaft mode, Schema Registry, topic creation, and broker configuration. Establishes the foundational messaging layer and demonstrates core Kafka concepts for the certification exam.

### Epic 1: User Stories

- As a developer, I want Kafka brokers configured with proper replication so that data is durable **(8 - 12 hours)**
- As a developer, I want topics pre-created with correct partitioning so that data flows correctly **(6 - 10 hours)**
- As a developer, I want Schema Registry running so that I can validate message formats **(4 - 8 hours)**

### Epic 1: Tasks

- [ ] Configure Kafka brokers (3-node cluster) with KRaft mode
- [ ] Set up replication factor of 3 for all critical topics
- [ ] Create `traffic_raw` topic (6 partitions, keyed by road_segment)
- [ ] Create `traffic_processed` topic (6 partitions)
- [ ] Create `traffic_alerts` topic (3 partitions, short retention)
- [ ] Create `road_metadata` topic (1 partition, compacted)
- [ ] Create `traffic_dlq` dead-letter topic
- [ ] Configure Min ISR settings
- [ ] Set up Schema Registry with Avro and JSON support
- [ ] Configure retention policies per topic
- [ ] Enable lz4 compression at broker level
- [ ] Create docker-compose.yml with Kafka stack (brokers + Schema Registry)
- [ ] Demonstrate CLI tools: kafka-topics, kafka-console-producer, kafka-console-consumer, kafka-configs, kafka-acls

### Epic 1: Definition of Done

- [ ] All Kafka brokers running and healthy (verified via `kafka-broker-api-versions`)
- [ ] Schema Registry accessible and responding to schema registration requests
- [ ] All 5 topics created with correct partition counts and replication factors
- [ ] Topic configurations verified via `kafka-configs --describe`
- [ ] Docker Compose starts entire stack cleanly
- [ ] `kafka-topics --list` shows all required topics
- [ ] Schema Registry can register and retrieve Avro schemas
- [ ] CLI tools demonstrated for all operations

---

## Epic 2: Java Kafka Producer (Data Ingestion)

**Epic Owner:** Backend Team
**Priority:** Critical
**Target Release:** Sprint 2-3
**Exam Coverage:** Section 2 — Application Development (28%)

### Epic 2: Description

Build a production-grade Java Kafka producer that publishes raw sensor events to `traffic_raw` topic with proper partitioning, compression, batching, error handling, serialization, and security.

### Epic 2: User Stories

- As a system, I want sensor events published to Kafka so that downstream processors can consume them **(12 - 18 hours)**
- As a developer, I want proper error handling so that transient failures do not lose data **(8 - 14 hours)**
- As an operator, I want metrics on producer performance so that I can tune throughput **(6 - 10 hours)**

### Epic 2: Tasks

- [ ] Create `Producer.java` with KafkaProducer client
- [ ] Implement key hashing partitioning strategy (key by road_segment)
- [ ] Configure lz4/snappy compression
- [ ] Tune batch.size and linger.ms for throughput
- [ ] Implement retry logic with retryable vs non-retryable error classification
- [ ] Add SSL/SASL authentication configuration
- [ ] Implement callback handlers for acknowledgment tracking
- [ ] Add JMX metrics for producer monitoring
- [ ] Create Avro schema for raw sensor events
- [ ] Implement serialization with Schema Registry integration
- [ ] Add dead-letter topic routing for failed messages
- [ ] Create producer configuration class with environment-based overrides
- [ ] Write unit tests with Embedded Kafka

### Epic 2: Definition of Done

- [ ] Producer successfully publishes to `traffic_raw` topic
- [ ] Messages correctly keyed by `road_segment` (verified via partition inspection)
- [ ] Compression enabled and verified in broker metrics
- [ ] Batching configured: batch.size=32KB, linger.ms=50
- [ ] Retry mechanism handles transient failures
- [ ] Non-retryable errors routed to `traffic_dlq`
- [ ] SSL/SASL authentication working
- [ ] JMX metrics exposed for: record-send-rate, batch-size-avg, error-rate
- [ ] Avro schema registered in Schema Registry
- [ ] Unit tests pass with Embedded Kafka (80%+ coverage)
- [ ] Integration test sends 10,000 messages and verifies all arrive
- [ ] Throughput benchmark: 10,000+ messages/second

---

## Epic 3: Kafka Streams Processor (Stream Processing + Alerting)

**Epic Owner:** Backend Team
**Priority:** High
**Target Release:** Sprint 3-5
**Exam Coverage:** Section 3 — Kafka Streams (12%) + Section 2 — Application Development (28%)

### Epic 3: Description

Build Kafka Streams application that consumes from `traffic_raw`, performs windowed aggregations with state stores, calculates congestion scores, and emits alerts to `traffic_alerts` using exactly-once semantics. This single component covers both Kafka Streams and advanced application development topics.

### Epic 3: User Stories

- As a data analyst, I want windowed aggregations so that I can see traffic trends over time **(15 - 22 hours)**
- As an operator, I want real-time alerts generated so that stakeholders are notified of congestion **(10 - 16 hours)**
- As a developer, I want state stores so that historical trends are available for analysis **(8 - 14 hours)**
- As an operator, I want exactly-once processing so that no alerts are duplicated or lost **(6 - 10 hours)**

### Epic 3: Tasks

- [ ] Create `TrafficStreamsApp.java` with Kafka Streams topology
- [ ] Configure KStream from `traffic_raw`
- [ ] Implement GroupBy road_segment
- [ ] Add tumbling window aggregation (10-second windows)
- [ ] Add sliding window aggregation (10s window, 5s slide) for smoother averages
- [ ] Create state store for historical congestion tracking
- [ ] Calculate average speed per window
- [ ] Calculate vehicle density per window
- [ ] Implement congestion score formula: `(max_speed - window_avg_speed) / max_speed`
- [ ] Implement alert emission logic (congestion > 0.7 emits HIGH alert)
- [ ] Configure exactly-once semantics (`processing.guarantee=exactly_once_v2`)
- [ ] Add dead-letter topic routing for failed processing
- [ ] Implement retriable vs non-retriable error classification
- [ ] Add KTable for road metadata enrichment
- [ ] Configure state store cleanup and retention
- [ ] Add JMX metrics for Streams monitoring
- [ ] Write topology unit tests with TopologyTestDriver
- [ ] Write integration tests with Embedded Kafka

### Epic 3: Definition of Done

- [ ] Streams app consumes from `traffic_raw` and writes enriched events to `traffic_processed`
- [ ] Streams app emits alerts to `traffic_alerts` when thresholds exceeded
- [ ] State store maintains congestion history per road segment
- [ ] Exactly-once semantics verified (no duplicate alerts under failure scenarios)
- [ ] Alert threshold logic works: congestion_score > 0.7 emits HIGH alert
- [ ] State store recovers correctly after application restart
- [ ] Dead-letter topic receives failed messages
- [ ] JMX metrics exposed: commit-rate, poll-rate, processing-latency
- [ ] Topology description matches design
- [ ] Unit tests verify topology with TopologyTestDriver
- [ ] Integration test with Embedded Kafka processes 1,000 events
- [ ] No data loss under broker restart scenario

---

## Epic 4: Kafka Connect Integration

**Epic Owner:** Platform Team
**Priority:** Medium
**Target Release:** Sprint 5-6
**Exam Coverage:** Section 4 — Kafka Connect (15%)

### Epic 4: Description

Implement Kafka Connect source and sink connectors for PostgreSQL integration, demonstrating source, sink, and CDC connector concepts required for the certification exam.

### Epic 4: User Stories

- As a system, I want road metadata available in Kafka so that enrichments can occur **(8 - 12 hours)**
- As an operator, I want alerts persisted to PostgreSQL so that historical queries are possible **(6 - 10 hours)**
- As a developer, I want connector configurations managed declaratively so that changes are version-controlled **(4 - 8 hours)**

### Epic 4: Tasks

- [ ] Configure PostgreSQL source connector for road metadata
- [ ] Create `source-config.json` with JDBC connector settings
- [ ] Configure PostgreSQL sink connector for alerts
- [ ] Create `sink-config.json` with JDBC connector settings
- [ ] Set up Kafka Connect REST API integration
- [ ] Implement connector health monitoring
- [ ] Configure schema compatibility modes
- [ ] Add error handling with dead-letter queue routing
- [ ] Create PostgreSQL schema for road_metadata and alerts tables
- [ ] Test connector restart and offset recovery
- [ ] Write unit tests for connector configurations

### Epic 4: Definition of Done

- [ ] Source connector reads road metadata from PostgreSQL to `road_metadata` topic
- [ ] Sink connector writes alerts from `traffic_alerts` to PostgreSQL
- [ ] Connector configurations stored in version-controlled JSON files
- [ ] Connect REST API returns connector status (RUNNING/FAILED/PAUSED)
- [ ] Connectors recover correctly after restart
- [ ] Error messages routed to dead-letter topic
- [ ] PostgreSQL tables created with correct schema
- [ ] Data flows end-to-end: PostgreSQL to Kafka to PostgreSQL
- [ ] Unit tests verify connector configurations

---

## Epic 5: Dashboard (Spring Boot)

**Epic Owner:** Backend Team
**Priority:** Medium
**Target Release:** Sprint 6-7
**Exam Coverage:** Section 2 — Application Development (28%)

### Epic 5: Description

Build a simple Spring Boot dashboard with REST API and server-rendered Thymeleaf views to visualize traffic conditions, alerts, and basic Kafka metrics. No frontend framework required.

### Epic 5: User Stories

- As a user, I want to see current traffic conditions so that I can monitor the system **(12 - 18 hours)**
- As a user, I want to see active alerts so that I can respond to issues **(8 - 12 hours)**
- As an operator, I want basic Kafka metrics so that I can monitor system health **(6 - 10 hours)**

### Epic 5: Tasks

- [ ] Create `DashboardApplication.java` (Spring Boot)
- [ ] Build REST API endpoints for traffic data
- [ ] Create Thymeleaf templates for traffic view
- [ ] Create Thymeleaf templates for alerts view
- [ ] Implement polling-based data refresh (every 5 seconds)
- [ ] Add basic Kafka metrics endpoint
- [ ] Configure CORS and error handling
- [ ] Add responsive CSS styling

### Epic 5: Definition of Done

- [ ] Spring Boot application starts and serves REST API
- [ ] Traffic view displays current conditions from `traffic_processed`
- [ ] Alerts view displays active alerts from `traffic_alerts`
- [ ] Data refreshes automatically via polling
- [ ] Basic Kafka metrics displayed
- [ ] No server errors
- [ ] Unit tests pass for API endpoints

---

## Epic 6: Security Implementation

**Epic Owner:** Security Team
**Priority:** High
**Target Release:** Sprint 3-5 (Parallel with processing)
**Exam Coverage:** Section 1 — Kafka Fundamentals (23%)

### Epic 6: Description

Implement SSL encryption, SASL/SCRAM authentication, and ACLs for all Kafka clients and connectors. Security is a core certification topic and is integrated into the producer and streams epics.

### Epic 6: User Stories

- As an operator, I want SSL encryption so that data is protected in transit **(10 - 16 hours)**
- As an administrator, I want SASL authentication so that only authorized clients can connect **(8 - 12 hours)**
- As a security officer, I want ACLs so that access is controlled per topic **(6 - 10 hours)**

### Epic 6: Tasks

- [ ] Generate SSL certificates for Kafka brokers
- [ ] Configure SSL listener on Kafka brokers
- [ ] Set up SASL/SCRAM authentication mechanism
- [ ] Create user credentials for each client application
- [ ] Configure ACLs for all topics (producer: write, consumer: read)
- [ ] Update producer with SSL/SASL configuration
- [ ] Update Streams app with SSL/SASL configuration
- [ ] Update Kafka Connect with SSL/SASL configuration
- [ ] Update Schema Registry with SSL configuration
- [ ] Test secure communication end-to-end
- [ ] Document security configuration

### Epic 6: Definition of Done

- [ ] SSL certificates generated and installed on all brokers
- [ ] SASL/SCRAM authentication enabled on broker listeners
- [ ] ACLs configured: each client has minimum required permissions
- [ ] Producer connects with SSL/SASL and publishes successfully
- [ ] Streams app connects with SSL/SASL and processes successfully
- [ ] Unauthorized access attempts rejected (test with wrong credentials)
- [ ] Schema Registry accessible over SSL
- [ ] Kafka Connect operates with security enabled
- [ ] `kafka-acls --list` shows correct ACL entries
- [ ] Security configuration documented

---

## Epic 7: Observability & Monitoring

**Epic Owner:** Platform Team
**Priority:** Medium
**Target Release:** Sprint 6-8
**Exam Coverage:** Section 6 — Application Observability (13%)

### Epic 7: Description

Set up Prometheus metrics collection, Grafana dashboards, and JMX exporters for comprehensive system monitoring.

### Epic 7: User Stories

- As an operator, I want Prometheus collecting metrics so that I can monitor system health **(8 - 12 hours)**
- As an operator, I want Grafana dashboards so that I can visualize trends **(10 - 16 hours)**
- As an on-call engineer, I want alerting rules so that I am notified of issues **(6 - 10 hours)**

### Epic 7: Tasks

- [ ] Configure JMX exporter for Kafka brokers
- [ ] Configure JMX exporter for Java applications
- [ ] Set up Prometheus scraper configuration
- [ ] Create Prometheus alerting rules for Kafka metrics
- [ ] Build Grafana dashboard: Kafka Cluster Overview
- [ ] Build Grafana dashboard: Producer/Consumer Performance
- [ ] Build Grafana dashboard: Streams Processing Metrics
- [ ] Configure Prometheus retention and storage
- [ ] Add custom application metrics (business metrics)

### Epic 7: Definition of Done

- [ ] Prometheus scraping all Kafka and application metrics
- [ ] Grafana dashboards display real-time metrics
- [ ] Kafka Cluster Dashboard shows: broker status, topic throughput, partition distribution
- [ ] Producer Dashboard shows: send rate, error rate, batch size
- [ ] Consumer Dashboard shows: lag, throughput, commit rate
- [ ] Streams Dashboard shows: processing rate, commit rate, state store size
- [ ] Prometheus alerting rules trigger on: broker down, high consumer lag, error spike
- [ ] Custom business metrics visible: alerts/minute, congestion events/hour
- [ ] Dashboard refresh interval 15 seconds or less

---

## Epic 8: Testing, Deployment & Documentation

**Epic Owner:** DevOps Team
**Priority:** High
**Target Release:** Sprint 7-9
**Exam Coverage:** Section 5 — Application Testing (8%)

### Epic 8: Description

Implement comprehensive testing strategy, Docker containerization, CI/CD pipeline, and project documentation. Testing covers the certification exam's application testing section.

### Epic 8: User Stories

- As a developer, I want unit tests so that code changes do not break functionality **(10 - 16 hours)**
- As a developer, I want integration tests so that component interactions work correctly **(10 - 16 hours)**
- As a developer, I want Docker images so that deployment is consistent **(8 - 12 hours)**
- As a team, I want CI/CD so that code changes are automatically tested **(12 - 18 hours)**
- As a new developer, I want documentation so that I can contribute quickly **(8 - 12 hours)**

### Epic 8: Tasks

- [ ] Set up JUnit 5 test framework
- [ ] Configure Embedded Kafka for unit tests
- [ ] Set up Testcontainers for integration tests
- [ ] Write producer unit tests (serialization, partitioning, error handling)
- [ ] Write Streams topology unit tests (TopologyTestDriver)
- [ ] Write alert logic tests
- [ ] Create integration test: producer to Kafka to Streams
- [ ] Create integration test: Kafka Connect with Testcontainers PostgreSQL
- [ ] Implement schema validation tests
- [ ] Set up load testing with kafka-producer-perf-test
- [ ] Create Dockerfile for Java producer
- [ ] Create Dockerfile for Kafka Streams app
- [ ] Create Dockerfile for Spring Boot dashboard
- [ ] Update docker-compose.yml with all services
- [ ] Set up GitHub Actions workflow for CI
- [ ] Configure automated test execution in CI
- [ ] Create architecture diagram (Mermaid)
- [ ] Document Kafka topic schema and partitioning strategy
- [ ] Write API documentation for dashboard REST endpoints
- [ ] Create developer setup guide

### Epic 8: Definition of Done

- [ ] Unit test coverage 80%+ for all Java components
- [ ] All unit tests pass with Embedded Kafka
- [ ] Integration tests use Testcontainers (no external dependencies)
- [ ] Producer integration test: publish and consume 10,000 messages
- [ ] Streams integration test: verify topology with TopologyTestDriver
- [ ] Connect integration test: source and sink with Testcontainers PostgreSQL
- [ ] Schema validation tests verify all message formats
- [ ] Load test: producer achieves 10,000+ messages/second
- [ ] All Java components have Docker images
- [ ] `docker-compose up -d` starts entire stack
- [ ] All services healthy and communicating
- [ ] GitHub Actions workflow runs on every PR
- [ ] Architecture diagram shows all components and data flow
- [ ] Developer setup guide enables new developer to run system in under 30 minutes

---

## Epic Summary

| Epic | Title                       | Priority | Sprints  | Hours         |
| ---- | -------                     | -------- |--------- | ------------  |
| 1    | Kafka Infrastructure        | Critical | 1-2      | 18 - 30       |
| 2    | Java Kafka Producer         | Critical | 2-3      | 26 - 42       |
| 3    | Kafka Streams Processor     | High     | 3-5      | 39 - 62       |
| 4    | Kafka Connect               | Medium   | 5-6      | 18 - 30       |
| 5    | Dashboard (Spring Boot)     | Medium   | 6-7      | 26 - 40       |
| 6    | Security                    | High     | 3-5      | 24 - 38       |
| 7    | Observability               | Medium   | 6-8      | 24 - 38       |
| 8    | Testing, Deployment & Docs  | High     | 7-9      | 48 - 76       |
|      | **TOTAL**                   |          |          | **223 - 356** |

---

## Certification Topic Coverage Matrix

| Exam Section                         |  %  | Epics Covering         |
| ---                                  | --- |  ---                   |
| Section 1: Kafka Fundamentals        | 23% | Epic 1, Epic 6         |
| Section 2: Application Development   | 28% | Epic 2, Epic 3, Epic 5 |
| Section 3: Kafka Streams             | 12% | Epic 3                 |
| Section 4: Kafka Connect             | 15% | Epic 4                 |
| Section 5: Application Testing       | 8%  | Epic 8                 |
| Section 6: Application Observability | 13% | Epic 7                 |

All six exam sections are covered. Every certification task from the exam outline is addressed:

- [x] Connect to a secured cluster (Epic 1, Epic 6)
- [x] Produce and consume from topics (Epic 2, Epic 3)
- [x] Model datasets (Epic 1, Epic 2)
- [x] Define topic configurations (Epic 1)
- [x] Understand compression (Epic 2)
- [x] Understand Admin API (Epic 1)
- [x] Deploy clients (Epic 2, Epic 3)
- [x] Test client applications (Epic 8)
- [x] Tune applications (Epic 2, Epic 3)
- [x] Write streams applications (Epic 3)
- [x] Serialization and deserialization (Epic 2, Epic 3)
- [x] Monitor applications (Epic 7)
- [x] Exactly-once / at-least-once semantics (Epic 3)
- [x] Configure and deploy connectors (Epic 4)
- [x] Troubleshoot applications (Epic 8)
- [x] Work with CLI tools (Epic 1)
- [x] Understand partitions, topics, ordering (Epic 1)
- [x] Understand sink, source, and CDC (Epic 4)
