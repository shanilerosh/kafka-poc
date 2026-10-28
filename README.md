# RideFlow — learn Kafka + Spring Boot by building a mini Uber

A runnable proof-of-concept. Every Kafka concept you need is demonstrated by one small piece of a ride-hailing
backend, with comments in the code explaining *why*. Read this file once, then read the code in the order of the
[concept map](#2-concept-map--where-to-look).

## 0. Run it (5 minutes)

Requires Java 21, Maven, Docker.

```bash
docker compose up -d          # 3 Kafka brokers (KRaft) + Kafka UI at http://localhost:8085
mvn spring-boot:run           # the app, on http://localhost:8080
```

Open **http://localhost:8085** (Kafka UI) next to your terminal. Watching topics, partitions, messages and consumer
groups change while you run the labs is the fastest way to learn.

`docker compose down -v` wipes everything (all messages, offsets, topics) for a clean start.

---

## 1. The story: Kafka = the city's dispatch radio

Imagine Uber *without* Kafka: when a rider requests a trip, the API calls the matching service, which calls
notifications, which calls billing... Each service must be up, fast, and know about the others. One slow service stalls everyone.

With Kafka, services don't call each other. They **announce facts** ("ride requested") on a shared, durable log, and
anyone interested **listens**.

| Kafka concept | Uber analogy | In this repo |
|---|---|---|
| **Event** | "Ride ride-42 was requested", a fact that already happened | `domain/Events.java` |
| **Topic** | A radio channel: `ride.requested`, `payment.completed` | `config/Topics.java` |
| **Partition** | A channel split into lanes so many dispatchers can work in parallel | `TopicsConfig` (6 partitions) |
| **Offset** | The position of a message in a lane, like a line number in a ledger | every `[producer]` log line |
| **Producer** | The rider app / any service announcing something | `producer/EventPublisher` |
| **Key** | The ride ID written on the envelope: same key → same lane → in order | `EventPublisher.publish` |
| **Consumer** | A service listening to a channel | `consumer/*` |
| **Consumer group** | A team of dispatchers sharing one channel's lanes; each message goes to one team member | `groupId = "matching-service"` |
| **Fan-out** | Billing, notifications and analytics each hear *every* message, independently | several groups on one topic |
| **Broker / cluster** | The radio towers; if one fails the others carry on | `docker-compose.yml` |
| **Replication / ISR** | Every lane is copied to 3 towers; leader + in-sync followers | `replicas(3)`, `min.insync.replicas=2` |
| **Retention** | How long the ledger keeps pages (7 days; GPS pings 1 hour) | `TopicsConfig` |
| **Log compaction** | A whiteboard: only the latest status per driver is kept | `driver.status` |

### Architecture

```
                         ┌──────────────── Kafka cluster (3 brokers) ────────────────┐
 REST API                │                                                           │
 POST /api/rides ──────► │ ride.requested ──┬──► matching-service (group, 3 threads) │
 (EventPublisher)        │   (6 partitions) │         │ finds driver via DriverRegistry
                         │                  │         ├──► driver.status (compacted)──┼──► DriverRegistry (rebuilds on boot)
                         │                  │         └──► ride.matched ──┬───────────┼──► notification-matched (non-blocking retry)
                         │                  │                             └───────────┼──► ride-tracker (read model for GET /rides)
 POST .../complete ────► │ ride.completed ──┬──► billing-service (TRANSACTIONAL) ──────┼──► payment.completed ──► notification-receipts
                         │                  └──► driver-release-service → driver.status│
 Driver simulator ─────► │ driver.location ────► analytics-service (BATCH)             │
                         │                                                             │
                         │ ride.requested ──► Kafka Streams ──► surge.pricing ─────────┼──► SurgeCache ──► used by billing
                         │                    (windowed count)  + queryable state store│
                         │ *.DLT topics ◄── failures ──────────► DeadLetterMonitor     │
                         └─────────────────────────────────────────────────────────────┘
```

Note what the REST controller does **not** do: it never calls matching or billing. It publishes a fact and returns.

---

## 2. Concept map — where to look

Read in this order.

| # | Concept | File | What to notice |
|---|---|---|---|
| 1 | Cluster, KRaft, listeners, replication of internal topics | `docker-compose.yml` | 3 nodes, each broker+controller; `INTERNAL` vs `EXTERNAL` listener |
| 2 | Topics, partitions, replication, retention, compaction, `min.insync.replicas` | `config/TopicsConfig.java` | One decision per topic, justified in comments |
| 3 | Producer: key → partition, headers, async send + callback | `producer/EventPublisher.java` | `send()` returns a future; keys are rideId / driverId |
| 4 | Producer reliability: `acks=all`, idempotence, batching (`linger.ms`), compression | `application.yml` | Why each setting exists |
| 5 | Serialization (JSON), trusted packages, type headers | `config/KafkaClientConfig.java` | `ErrorHandlingDeserializer` protects against poison pills |
| 6 | Consumer groups, partitions assignment, scaling (`concurrency`) | `consumer/MatchingService.java` | 3 threads × 2 partitions each |
| 7 | Offsets & commit semantics (at-least-once) | `application.yml`, `AckMode.RECORD` in `KafkaClientConfig` | Offset committed *after* your code succeeds |
| 8 | Fan-out: many groups, one topic | `ride.matched` consumers | `/api/admin/groups` shows independent offsets |
| 9 | Compacted topic as a table; manual partition assignment & replay | `consumer/DriverRegistry.java` | Rebuilds state from the topic on every boot |
| 10 | Blocking retry + Dead Letter Topic | `KafkaClientConfig.errorHandler`, `DeadLetterMonitor` | Fatal errors skip retries |
| 11 | Non-blocking retry (`@RetryableTopic`) | `consumer/NotificationService.java` | Retry topics keep the main lane flowing |
| 12 | Poison pill | `api/DemoController.poisonPill` | Garbage bytes → DLT, app lives on |
| 13 | Batch consumption | `consumer/AnalyticsService.java` | `List<…>` per poll; throughput vs latency |
| 14 | Exactly-once / transactions | `consumer/BillingService.java` + `transactionalListenerFactory` | Output + offset commit atomically; `read_committed` |
| 15 | Idempotent consumer | `BillingService.alreadyBilled` | Kafka transactions can't roll back a card charge |
| 16 | Multi-topic listener, ordering is per-partition only | `consumer/RideTracker.java` | `@KafkaHandler`; events can arrive out of order across topics |
| 17 | Kafka Streams: KStream/KTable, repartition, windowing, state stores, changelog topics, EOS | `streams/SurgePricingTopology.java` | Look at the `rideflow-streams-*` topics in Kafka UI |
| 18 | Interactive queries | `streams/ZoneDemandQuery.java` | Reads state straight from the Streams app |
| 19 | Admin API: topics, leaders, ISR, consumer lag | `admin/KafkaInspector.java` | Same data Kafka UI shows |
| 20 | Rebalancing, failover | Labs 6 and 7 below | |

---

## 3. Labs — run these in order

A tip: keep a terminal on the app log and Kafka UI open at the same time.

### Lab 1 — Produce, consume, fan-out
```bash
curl -s -XPOST localhost:8080/api/rides -H 'content-type: application/json' \
  -d '{"riderId":"rider-alice","pickupZone":"DOWNTOWN","dropoffZone":"AIRPORT"}'
```
The response tells you the **partition** and **offset** the record landed on. In the log you'll see matching →
notification → ride-tracker all handle the same event. Then:
```bash
curl -s localhost:8080/api/rides/<rideId> | python3 -m json.tool      # history shows topic[partition@offset] per event
curl -s -XPOST localhost:8080/api/rides/<rideId>/complete -H 'content-type: application/json' -d '{"distanceKm":12}'
```
Completing triggers billing (in a transaction), a receipt, and frees the driver.

### Lab 2 — Keys and ordering
Request several rides and look at the partitions. Different rideIds spread over partitions. In Kafka UI open
`driver.location`: all messages for `driver-1` are on **one** partition (key = driverId), so a driver's pings are strictly ordered.

### Lab 3 — Compaction (topic as a table)
```bash
curl -s localhost:8080/api/drivers          # the in-memory table built from driver.status
```
Restart the app: the table is rebuilt by replaying `driver.status` from offset 0. Request a ride in `AIRPORT`:
a driver flips to `ON_TRIP`. In Kafka UI → `driver.status` → Messages: after a minute or so, older records for the same
key disappear (compaction runs on closed segments; we set `segment.ms=10s` to make it quick).

### Lab 4 — Failures: three kinds, three behaviours
| Try | What happens | Why |
|---|---|---|
| Ride with `"pickupZone":"NOWHERE"` | log shows "got ride…" 4 times (1 + 3 retries with 0.5s, 1s, 2s backoff), then a `[DLT]` line | **Blocking retry** → Dead Letter Topic. The partition is stuck during retries. |
| Rider `flaky-anna` | SMS "fails" twice (read from `ride.matched-retry-1000`, then `-2000`), succeeds on attempt 3 | **Non-blocking retry**: other rides keep flowing |
| Rider `deadsms-bob` | same, but ends in `ride.matched-dlt` and "GAVE UP" | Retries exhausted |
| `curl -XPOST localhost:8080/api/demo/poison-pill` | `[DLT] … reason="failed to deserialize"`, **no retries**, app unaffected | Retrying can't fix bad bytes |

(The long stack traces in the log when retries are exhausted are expected.)

### Lab 5 — Exactly-once and surge pricing
1. Rider `declined-dan`: request a ride in `SUBURBS`, then complete it. Billing throws *inside a transaction*: nothing
   reaches `payment.completed`, the offset isn't committed, the record is redelivered twice, then lands in `ride.completed.DLT`.
2. `curl -XPOST 'localhost:8080/api/demo/burst?zone=DOWNTOWN&count=12'` then `curl localhost:8080/api/surge`
   and `curl localhost:8080/api/zones/demand`. Kafka Streams counts requests per zone in 30-second windows and publishes
   a multiplier to `surge.pricing`. Complete a ride right after and see the higher fare.
   Try `count=30`: only 15 drivers per zone, so the rest fail to match and end up in the DLT.
   In Kafka UI, look at the `rideflow-streams-*-repartition` and `-changelog` topics Streams created for itself.

### Lab 6 — Scale out and watch a rebalance
In a second terminal run another instance on another port:
```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081
```
Watch the first instance log `partitions revoked` / `assigned`. Now each instance has ~half of the partitions of `ride.requested`.
`curl localhost:8080/api/admin/groups` shows 6 members in `matching-service` (2 instances × 3 threads = 6 consumers for 6
partitions, so exactly one partition each; a 7th consumer would sit idle). Stop one instance: the other takes everything back. (We use the *cooperative* assignor, so only
the moved partitions pause, not the whole group.)
Note both instances run their own driver simulator; set `--rideflow.simulator.enabled=false` on the second one.

### Lab 7 — Kill a broker
```bash
curl -s localhost:8080/api/admin/topics | python3 -m json.tool      # note leader / replicas / isr per partition
docker stop kafka-3
curl -s localhost:8080/api/admin/topics ...                         # ISR shrinks to [1, 2], leaders move off broker 3
curl -XPOST localhost:8080/api/rides ...                            # still works: 2 in-sync replicas >= min.insync.replicas
docker stop kafka-2                                                 # only 1 of 3 nodes left
curl -XPOST localhost:8080/api/rides ...                            # does NOT succeed: Kafka refuses writes it can't replicate
                                                                    # (min.insync.replicas=2) rather than risk losing data.
                                                                    # Expect a timeout (up to delivery.timeout.ms = 45s).
                                                                    # Here all 3 nodes are also KRaft controllers, so quorum is lost too.
docker start kafka-2 kafka-3                                        # everything heals
```

---

## 4. REST cheat sheet

| Call | Purpose |
|---|---|
| `POST /api/rides` `{riderId, pickupZone, dropoffZone}` | Publish `RideRequested`; returns partition + offset |
| `POST /api/rides/{id}/complete` `{distanceKm}` | Publish `RideCompleted` |
| `GET /api/rides`, `/api/rides/{id}` | Read model built from events |
| `GET /api/drivers` | Table built from the compacted topic |
| `GET /api/surge`, `/api/zones/demand` | Kafka Streams output / interactive query |
| `GET /api/admin/topics`, `/api/admin/groups` | Partitions, leaders, ISR; groups, members, lag |
| `POST /api/demo/burst`, `/api/demo/poison-pill` | Experiments |

Zones: `DOWNTOWN`, `AIRPORT`, `SUBURBS` (15 simulated drivers each). Drivers that were matched stay `ON_TRIP` until you complete
the ride; restarting the app re-seeds everyone as available.

---

## 5. Things worth knowing

- **Why does a group show "lag=1" when idle?** Transactions (billing and Streams) write a commit marker into the partition
  that occupies an offset. It's not a real message.
- **Why `driver.location` has 6 partitions but `driver.status` has 3?** Partitions are a throughput/parallelism decision.
  Status updates are rare. Also: you can add partitions later but **never remove** them, and adding changes key → partition mapping.
- **Max parallelism of a group = number of partitions.** A 7th consumer in a 6-partition group idles.
- **At-most-once / at-least-once / exactly-once:** commit offset before processing (can lose) / after (can duplicate; our default)
  / transactions + idempotent producers (Kafka→Kafka only; external side effects still need idempotency).
- **Kafka is not a queue.** Messages aren't deleted when read; each group tracks its own offset, and you can replay by resetting it.

## 6. Deliberately left out (ask and I'll add them)

- **Schema Registry + Avro/Protobuf**: contract enforcement and evolution for message formats. Here we use JSON for readability.
- **Kafka Connect**: ready-made connectors that move data between Kafka and databases / S3 without code (e.g. stream Postgres changes in with Debezium).
- **Security** (TLS, SASL, ACLs), multi-datacenter replication (MirrorMaker), monitoring (JMX / Prometheus).
- **Tests** with Testcontainers.

## 7. Known simplifications (a POC, not production)

- `DriverRegistry` is per instance, so two app instances could each match the same driver. A real system would keep this state
  in a partitioned store (or have matching be a Kafka Streams app keyed by zone, which makes it consistent by construction).
- `EventPublisher` does not retry or surface async publish failures beyond logging.
- Ride/driver state lives in memory and is rebuilt from topics; there is no database.
