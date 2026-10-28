package com.rideflow.producer;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.*;

/**
 * PRODUCER SIDE.
 *
 * Three things every send decides:
 *  1. TOPIC  - which stream of facts
 *  2. KEY    - decides the PARTITION (hash(key) % partitions). Same key => same partition => strict ordering.
 *              We key ride events by rideId and driver events by driverId.
 *  3. VALUE  - the event, serialized to JSON.
 * Plus optional HEADERS (metadata that is not part of the business payload, e.g. tracing).
 *
 * send() is ASYNCHRONOUS: it returns a future immediately; the record is batched (linger.ms) and sent in the background.
 */
@Component
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final KafkaTemplate<String, Object> kafka;

    public EventPublisher(KafkaTemplate<String, Object> kafka) {
        this.kafka = kafka;
    }

    public CompletableFuture<SendResult<String, Object>> rideRequested(RideRequested e) {
        return publish(Topics.RIDE_REQUESTED, e.rideId(), e);
    }

    public CompletableFuture<SendResult<String, Object>> rideMatched(RideMatched e) {
        return publish(Topics.RIDE_MATCHED, e.rideId(), e);
    }

    public CompletableFuture<SendResult<String, Object>> rideCompleted(RideCompleted e) {
        return publish(Topics.RIDE_COMPLETED, e.rideId(), e);
    }

    /** Key = driverId => all pings of one driver are ordered; different drivers spread over partitions. */
    public CompletableFuture<SendResult<String, Object>> driverLocation(DriverLocation e) {
        return kafka.send(Topics.DRIVER_LOCATION, e.driverId(), e);   // no logging: too chatty
    }

    /** Compacted topic: the key is the identity of the "row", so only the latest state per driver survives. */
    public CompletableFuture<SendResult<String, Object>> driverStatus(String driverId, String zone, DriverState state) {
        return publish(Topics.DRIVER_STATUS, driverId, new DriverStatusChanged(driverId, zone, state, Instant.now()));
    }

    private CompletableFuture<SendResult<String, Object>> publish(String topic, String key, Object event) {
        var record = new ProducerRecord<String, Object>(topic, key, event);
        record.headers().add("event-type", event.getClass().getSimpleName().getBytes(UTF_8));
        record.headers().add("trace-id", UUID.randomUUID().toString().getBytes(UTF_8));

        return kafka.send(record).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[producer] FAILED to publish {} key={}: {}", topic, key, ex.getMessage());
            } else {
                var md = result.getRecordMetadata();
                log.info("[producer] {} key={} -> partition={} offset={}", topic, key, md.partition(), md.offset());
            }
        });
    }
}
