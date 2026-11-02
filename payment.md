# Payment producer, topic/s and event/s

## Events
All payment events go to one topic, keyed by `paymentId`, so every event for a payment lands on the same partition in order.

| Event | When |
|---|---|
| PAYMENT_INITIATED | user starts a payment |
| PAYMENT_AUTHORISED / PAYMENT_NOT_AUTHORISED | after authorisation |
| PAYMENT_VALIDATED / PAYMENT_INVALIDATED | only if authorised |
| PAYMENT_COMPLETED / PAYMENT_NOT_COMPLETED | only if validated |

Schema (JSON):
```
PAY-1001:{"eventId":"e-0001","eventType":"PAYMENT_INITIATED","paymentId":"PAY-1001","customerId":"CUST-501","accountFrom":"1234567890","accountTo":"9876543210","amount":1500.00,"currency":"ZAR","channel":"APP","paymentType":"EFT","timestamp":"2026-11-01T10:15:30Z","reason":null}
```
`reason` is filled in on the failure events (e.g. INSUFFICIENT_FUNDS). `eventId` is unique per event so consumers can drop duplicates.

## Topic configs
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digital-payments.payment.events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=220924800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```
- Partitions: 1000Mb/s peak / 10Mb/s per consumer = 100 partitions, so a consumer group can scale to 100 instances. Producer side only needs 10 instances (1000 / 100).
- Replication 3 with min ISR 2: one broker can go down and writes still succeed with no data loss.
- Retention: 7 years (2557 days) for compliance.
- Delete, not compact: payment events are immutable history, compaction would throw away earlier states of a payment.
- lz4: cheap on CPU and fast, which matters for the 150ms budget. Same codec as the producer so the broker doesn't recompress.

### Consumer configs
Payment audit/reconciliation consumer. Reads every payment event, lag up to 1 hour is fine.
```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payments.payment.events \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group payment-audit-consumer \
  --consumer-property max.poll.records=1000 \
  --consumer-property session.timeout.ms=45000 \
  --consumer-property heartbeat.interval.ms=15000 \
  --consumer-property auto.offset.reset=earliest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=5000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=65536 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=2097152 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```
- earliest: audit can't miss anything, a new group must start from the beginning.
- Big batches and fetch.min.bytes since throughput matters more than latency here.
- Auto commit happens on the next poll, after the previous batch was processed, so it's at-least-once. Duplicates are dropped on `eventId`.

### Producer configs

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payments.payment.events \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property acks=all \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=5 \
  --producer-property batch.size=65536 \
  --producer-property delivery.timeout.ms=30000 \
  --producer-property request.timeout.ms=10000
```
- acks=all + idempotence: losing or duplicating a payment is not acceptable. Idempotence keeps ordering with up to 5 in-flight requests.
- Retries are effectively unlimited, delivery.timeout.ms caps it at 30s. After that the payment fails back to the caller instead of hanging.
- linger.ms=5 gives some batching without eating into the 150ms budget.
