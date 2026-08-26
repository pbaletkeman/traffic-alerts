# Pull Request

## Description

<!-- Briefly describe what this PR does and why. -->

## Related Epic

<!-- Link the related epic and user story. -->

- **Epic:** 
- **User Story:** 

## Type of Change

<!-- Check all that apply. -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to change)
- [ ] Refactoring (no functional changes)
- [ ] Documentation update
- [ ] Infrastructure / CI/CD change
- [ ] Configuration change
- [ ] Test addition or improvement

## Component(s) Affected

<!-- Check all components this PR touches. -->

- [ ] `producer/` — Kafka producer
- [ ] `spark/` — Spark Structured Streaming
- [ ] `streams/` — Kafka Streams processor
- [ ] `alert_engine/` — Alert consumer & business rules
- [ ] `connect/` — Kafka Connect connectors
- [ ] `dashboard/` — Spring Boot / React dashboard
- [ ] `observability/` — Prometheus / Grafana
- [ ] `config/` — Topic configuration
- [ ] Docker / deployment
- [ ] Tests

## How Has This Been Tested?

<!-- Describe the tests you ran and how to reproduce them. -->

- [ ] Unit tests pass (`mvn test` or `gradle test`)
- [ ] Integration tests pass (Testcontainers / Embedded Kafka)
- [ ] Manual testing with `docker-compose up`
- [ ] Kafka topics verified via `kafka-topics --describe`
- [ ] No regressions in existing functionality

## Checklist

- [ ] My code follows the project's coding style
- [ ] I have performed a self-review of my code
- [ ] I have commented my code where necessary (especially complex Kafka/Spark logic)
- [ ] I have updated documentation if applicable
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix/feature works
- [ ] New and existing unit tests pass locally
- [ ] Any dependent changes have been merged and published

## Screenshots / Logs

<!-- If applicable, add screenshots or relevant log output. -->

```

```

## Kafka Configuration Changes

<!-- If this PR modifies Kafka topic configs, producer/consumer settings, or security config, document them here. -->

| Setting | Old Value | New Value | Reason |
|---|---|---|---|
| | | | |
