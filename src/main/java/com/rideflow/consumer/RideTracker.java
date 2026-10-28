package com.rideflow.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.*;

/**
 * READ MODEL built purely from events (the "Q" in CQRS). The REST API reads ride state from here.
 *
 * One listener subscribed to FOUR topics, with a @KafkaHandler method per payload type.
 *
 * LESSON: Kafka only guarantees order WITHIN a partition. Events of one ride sit on four different topics, so
 * this consumer may see "matched" before "requested". That's why every handler is an upsert that can run in any order.
 */
@Component
@KafkaListener(groupId = "ride-tracker",
        topics = {Topics.RIDE_REQUESTED, Topics.RIDE_MATCHED, Topics.RIDE_COMPLETED, Topics.PAYMENT_COMPLETED})
public class RideTracker {

    public enum Status { REQUESTED, MATCHED, COMPLETED, PAID }

    public static class RideView {
        public final String rideId;
        public volatile String riderId, driverId, pickupZone, dropoffZone;
        public volatile Status status = Status.REQUESTED;
        public volatile Double fare;
        public final List<String> history = new CopyOnWriteArrayList<>();

        RideView(String rideId) { this.rideId = rideId; }

        synchronized void advance(Status s) {
            if (s.ordinal() > status.ordinal()) status = s;
        }
    }

    private final Map<String, RideView> rides = new ConcurrentHashMap<>();

    private RideView view(String id) {
        return rides.computeIfAbsent(id, RideView::new);
    }

    @KafkaHandler
    void on(RideRequested e, @Header(KafkaHeaders.RECEIVED_TOPIC) String t, @Header(KafkaHeaders.RECEIVED_PARTITION) int p, @Header(KafkaHeaders.OFFSET) long o) {
        var v = view(e.rideId());
        v.riderId = e.riderId(); v.pickupZone = e.pickupZone(); v.dropoffZone = e.dropoffZone();
        v.history.add("RideRequested from %s[p%d@%d]".formatted(t, p, o));
    }

    @KafkaHandler
    void on(RideMatched e, @Header(KafkaHeaders.RECEIVED_TOPIC) String t, @Header(KafkaHeaders.RECEIVED_PARTITION) int p, @Header(KafkaHeaders.OFFSET) long o) {
        var v = view(e.rideId());
        v.driverId = e.driverId(); v.riderId = e.riderId();
        v.advance(Status.MATCHED);
        v.history.add("RideMatched from %s[p%d@%d]".formatted(t, p, o));
    }

    @KafkaHandler
    void on(RideCompleted e, @Header(KafkaHeaders.RECEIVED_TOPIC) String t, @Header(KafkaHeaders.RECEIVED_PARTITION) int p, @Header(KafkaHeaders.OFFSET) long o) {
        var v = view(e.rideId());
        v.advance(Status.COMPLETED);
        v.history.add("RideCompleted from %s[p%d@%d]".formatted(t, p, o));
    }

    @KafkaHandler
    void on(PaymentCompleted e, @Header(KafkaHeaders.RECEIVED_TOPIC) String t, @Header(KafkaHeaders.RECEIVED_PARTITION) int p, @Header(KafkaHeaders.OFFSET) long o) {
        var v = view(e.rideId());
        v.fare = e.amount();
        v.advance(Status.PAID);
        v.history.add("PaymentCompleted from %s[p%d@%d]".formatted(t, p, o));
    }

    public Optional<RideView> find(String rideId) {
        return Optional.ofNullable(rides.get(rideId));
    }

    public Collection<RideView> all() {
        return rides.values();
    }
}
