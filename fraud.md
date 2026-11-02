# Fraud producer, consumer, topic/s and event/s

## Events
The fraud consumer reads PAYMENT_INITIATED from `digital-payments.payment.events`, scores the payment and the fraud producer publishes one of:

| Event | Score |
|---|---|
| FRAUD_SCORE_LOW | 0 - 39 |
| FRAUD_SCORE_MEDIUM | 40 - 69 |
| FRAUD_SCORE_HIGH | 70 - 100 |

Keyed by `paymentId`, same as the payment topic.
```
PAY-1001:{"eventId":"f-0001","eventType":"FRAUD_SCORE_LOW","paymentId":"PAY-1001","customerId":"CUST-501","score":12,"rulesTriggered":[],"timestamp":"2026-11-01T10:15:30.040Z"}
```

## Topic configs
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digital-payments.fraud.events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220924800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```
- 100 partitions, same sizing as the payment topic (1000Mb/s / 10Mb/s per consumer).
- RF 3 / min ISR 2 so a broker outage doesn't lose fraud decisions.
- 7 year retention, fraud events fall under the same compliance hold as payments.
- delete: every score is a record of a decision, nothing gets overwritten.
- lz4 for low latency, the whole path has to stay under 50ms.

### Consumer configs
Lag SLA < 50ms. The console consumer can't filter, so for the POC the output is piped through `grep '"eventType":"PAYMENT_INITIATED"'`.
```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payments.payment.events \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group fraud-scoring-consumer \
  --consumer-property max.poll.records=100 \
  --consumer-property session.timeout.ms=10000 \
  --consumer-property heartbeat.interval.ms=3000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=1000 \
  --consumer-property max.poll.interval.ms=30000 \
  --consumer-property fetch.min.bytes=1 \
  --consumer-property fetch.max.wait.ms=10 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```
- fetch.min.bytes=1 and fetch.max.wait.ms=10: return records as soon as they arrive, the default 500ms wait would blow the SLA.
- Small poll batches and a short session timeout so a dead instance is noticed quickly and its partitions move.
- earliest: no payment should skip fraud scoring.
- At-least-once is fine, scoring the same payment twice gives the same result. Downstream dedupes on `paymentId`.
- Scales up to 100 instances (one per partition). Cooperative sticky avoids stop-the-world rebalances.

### Producer configs

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payments.fraud.events \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property acks=all \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=0 \
  --producer-property batch.size=16384 \
  --producer-property delivery.timeout.ms=5000 \
  --producer-property request.timeout.ms=2000
```
- linger.ms=0: send immediately, latency matters more than batching here.
- acks=all + idempotence: a missing or duplicated HIGH score is a problem.
- delivery.timeout.ms=5000: a fraud score that arrives later than that is useless, better to fail and let the payment side handle it.
