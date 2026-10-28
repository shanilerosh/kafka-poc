package com.rideflow.consumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.PaymentCompleted;
import com.rideflow.domain.Events.RideMatched;

/**
 * FAN-OUT: ride.matched is read by several independent groups (ride-tracker, notification-matched, ...).
 * Each group gets EVERY message and keeps its OWN offsets. Kafka is not a queue that deletes on read.
 *
 * NON-BLOCKING RETRY (@RetryableTopic): on failure the record is moved to a retry topic
 * (ride.matched-retry-1000, -2000) and the main partition keeps flowing. After the last attempt it goes to
 * ride.matched-dlt where @DltHandler picks it up. Compare with MatchingService's *blocking* retry.
 *
 * Try it:  riderId "flaky-anna"  -> SMS gateway fails twice, succeeds on the 3rd attempt
 *          riderId "deadsms-bob" -> SMS gateway always fails -> ends in the DLT
 */
@Component
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0), numPartitions = "6", replicationFactor = "3")
    @KafkaListener(topics = Topics.RIDE_MATCHED, groupId = "notification-matched")
    void onRideMatched(RideMatched ride, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        int attempt = attempts.computeIfAbsent(ride.rideId(), k -> new AtomicInteger()).incrementAndGet();
        boolean fail = ride.riderId().startsWith("deadsms") || (ride.riderId().startsWith("flaky") && attempt < 3);
        if (fail) {
            log.warn("[notification] SMS gateway down for {} (attempt {}, read from {})", ride.riderId(), attempt, topic);
            throw new IllegalStateException("SMS gateway unavailable");
        }
        log.info("[notification] push to {}: driver {} is on the way (attempt {}, read from {})",
                ride.riderId(), ride.driverId(), attempt, topic);
    }

    @DltHandler
    void onRideMatchedDead(RideMatched ride, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("[notification] GAVE UP notifying {} about {} (arrived via {}). Alert a human.", ride.riderId(), ride.rideId(), topic);
    }

    @KafkaListener(topics = Topics.PAYMENT_COMPLETED, groupId = "notification-receipts")
    void onPayment(PaymentCompleted payment) {
        log.info("[notification] email receipt to {}: ${} for {}", payment.riderId(), payment.amount(), payment.rideId());
    }
}
