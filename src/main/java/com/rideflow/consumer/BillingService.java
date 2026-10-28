package com.rideflow.consumer;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.PaymentCompleted;
import com.rideflow.domain.Events.RideCompleted;

/**
 * EXACTLY-ONCE: consume ride.completed -> charge -> produce payment.completed, all in ONE Kafka transaction.
 *
 * Why it matters: with plain at-least-once delivery, a crash between "payment sent" and "offset committed"
 * would re-process the ride and charge twice. Inside a transaction the output message and the consumer offset
 * commit atomically, and downstream consumers (isolation.level=read_committed) never see aborted output.
 *
 * Two safety nets, because real systems have both:
 *   - the transaction (protects Kafka -> Kafka)
 *   - an idempotency check on rideId (protects external side effects such as a card charge, which a Kafka
 *     transaction can NOT roll back)
 *
 * Try it: complete a ride for a rider named "declined-carl". The listener throws, the transaction aborts
 * (nothing reaches payment.completed), the record is retried twice and then lands in ride.completed.DLT.
 */
@Component
public class BillingService {

    private static final Logger log = LoggerFactory.getLogger(BillingService.class);

    private final KafkaTemplate<String, Object> transactionalTemplate;
    private final SurgeCache surgeCache;
    private final Set<String> alreadyBilled = ConcurrentHashMap.newKeySet();

    public BillingService(@Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalTemplate,
                          SurgeCache surgeCache) {
        this.transactionalTemplate = transactionalTemplate;
        this.surgeCache = surgeCache;
    }

    @KafkaListener(topics = Topics.RIDE_COMPLETED, groupId = "billing-service", containerFactory = "transactionalListenerFactory")
    void onRideCompleted(RideCompleted ride) {
        if (alreadyBilled.contains(ride.rideId())) {
            log.warn("[billing] {} already billed, skipping duplicate", ride.rideId());
            return;
        }
        if (ride.riderId().startsWith("declined")) {
            log.warn("[billing] card declined for {} -> aborting transaction", ride.riderId());
            throw new IllegalStateException("Card declined");
        }

        double surge = surgeCache.multiplierFor(ride.pickupZone());
        double amount = Math.round((3.0 + ride.distanceKm() * 1.5 * surge) * 100) / 100.0;

        transactionalTemplate.send(Topics.PAYMENT_COMPLETED, ride.rideId(),
                new PaymentCompleted(ride.rideId(), ride.riderId(), ride.driverId(), amount, surge, Instant.now()));
        alreadyBilled.add(ride.rideId());
        log.info("[billing] charged {} ${} (surge x{}) inside a Kafka transaction", ride.riderId(), amount, surge);
    }
}
