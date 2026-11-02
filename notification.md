# Notification producer, consumer, topic/s and event/s

## Events
The notification consumer reads the failed events (PAYMENT_NOT_AUTHORISED, PAYMENT_INVALIDATED, PAYMENT_NOT_COMPLETED) and PAYMENT_VALIDATED from `digital-payments.payment.events`. The notification producer then publishes:

| Event | Trigger |
|---|---|
| NOTIFICATION_PAYMENT_APPROVED | PAYMENT_VALIDATED |
| NOTIFICATION_PAYMENT_FAILED | any of the failed events |

Keyed by `customerId` so a customer's notifications go out in order.
```
CUST-501:{"eventId":"n-0001","eventType":"NOTIFICATION_PAYMENT_APPROVED","paymentId":"PAY-1001","customerId":"CUST-501","channel":"PUSH","message":"Your payment of R1500.00 was approved","timestamp":"2026-11-01T10:15:31Z"}
```

## Topic configs
```bash
kubectl exec -it kafka-0 -- kafka-topics \
  --bootstrap-server kafka-service:9092 \
  --create \
  --topic digital-payments.notification.events \
  --partitions 100 \
  --replication-factor 3 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=2 \
  --config compression.type=lz4 \
  --config max.message.bytes=1048576
```
- 100 partitions, same throughput sizing as the other topics.
- RF 3 / min ISR 2, a dropped notification means an unhappy customer.
- 7 days retention: no compliance hold on notifications, a week is enough to replay or investigate.
- delete: notifications are events, not state.
- lz4, same as the other topics.

### Consumer configs
Lag SLA < 2s. Filtering for the POC is done with `grep -E '"eventType":"(PAYMENT_VALIDATED|PAYMENT_NOT_AUTHORISED|PAYMENT_INVALIDATED|PAYMENT_NOT_COMPLETED)"'` on the output.
```bash
kubectl exec -it kafka-0 -- kafka-console-consumer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payments.payment.events \
  --property print.key=true \
  --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
  --group notification-sender-consumer \
  --consumer-property max.poll.records=500 \
  --consumer-property session.timeout.ms=30000 \
  --consumer-property heartbeat.interval.ms=10000 \
  --consumer-property auto.offset.reset=latest \
  --consumer-property enable.auto.commit=true \
  --consumer-property auto.commit.interval.ms=1000 \
  --consumer-property max.poll.interval.ms=300000 \
  --consumer-property fetch.min.bytes=1024 \
  --consumer-property fetch.max.wait.ms=500 \
  --consumer-property max.partition.fetch.bytes=1048576 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor
```
- latest: the payment topic holds 7 years of data, starting a new group at earliest would send customers years of old notifications.
- fetch.min.bytes=1024 / fetch.max.wait.ms=500 gives a bit of batching and still stays well inside 2s.
- At-least-once. Sending a notification twice is annoying, so the sender dedupes on `paymentId` + event type. Missing one is worse.
- Scales up to 100 instances.

### Producer configs

```bash
kubectl exec -it kafka-0 -- kafka-console-producer \
  --bootstrap-server kafka-service:9092 \
  --topic digital-payments.notification.events \
  --property parse.key=true \
  --property key.separator=: \
  --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --property value.serializer=org.apache.kafka.common.serialization.StringSerializer \
  --producer-property acks=all \
  --producer-property retries=2147483647 \
  --producer-property max.in.flight.requests.per.connection=5 \
  --producer-property enable.idempotence=true \
  --producer-property compression.type=lz4 \
  --producer-property linger.ms=20 \
  --producer-property batch.size=65536 \
  --producer-property delivery.timeout.ms=30000 \
  --producer-property request.timeout.ms=10000
```
- acks=all is needed for idempotence, which stops duplicate notifications on retry.
- linger.ms=20: the 2s budget leaves room to batch more than the payment and fraud producers do.
