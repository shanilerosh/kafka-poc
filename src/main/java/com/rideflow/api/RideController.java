package com.rideflow.api;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import com.rideflow.consumer.RideTracker;
import com.rideflow.consumer.RideTracker.RideView;
import com.rideflow.domain.Events.RideCompleted;
import com.rideflow.domain.Events.RideRequested;
import com.rideflow.producer.EventPublisher;

/**
 * The "front door". Notice that it never calls the matching or billing code. It only PUBLISHES a fact and
 * returns. Everything else happens asynchronously, driven by events. That is the decoupling Kafka gives you.
 */
@RestController
@RequestMapping("/api/rides")
public class RideController {

    public record RideRequestBody(String riderId, String pickupZone, String dropoffZone) {}
    public record CompleteBody(Double distanceKm) {}

    private final EventPublisher publisher;
    private final RideTracker tracker;

    public RideController(EventPublisher publisher, RideTracker tracker) {
        this.publisher = publisher;
        this.tracker = tracker;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> requestRide(@RequestBody RideRequestBody body) throws Exception {
        if (body.riderId() == null || body.pickupZone() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "riderId and pickupZone are required");
        }
        String rideId = "ride-" + UUID.randomUUID().toString().substring(0, 8);
        var event = new RideRequested(rideId, body.riderId(), body.pickupZone(),
                body.dropoffZone() == null ? "DOWNTOWN" : body.dropoffZone(), Instant.now());

        // We block here only to SHOW you where the record landed. Real code usually would not wait.
        var md = publisher.rideRequested(event).get(10, TimeUnit.SECONDS).getRecordMetadata();

        var res = new LinkedHashMap<String, Object>();
        res.put("rideId", rideId);
        res.put("topic", md.topic());
        res.put("partition", md.partition());
        res.put("offset", md.offset());
        res.put("trackAt", "/api/rides/" + rideId);
        return res;
    }

    @PostMapping("/{rideId}/complete")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Map<String, Object> complete(@PathVariable String rideId, @RequestBody(required = false) CompleteBody body) throws Exception {
        RideView ride = tracker.find(rideId)
                .filter(r -> r.driverId != null)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.CONFLICT, "Ride is not matched with a driver (yet)"));
        double km = body == null || body.distanceKm() == null ? 5.0 : body.distanceKm();
        var md = publisher.rideCompleted(new RideCompleted(rideId, ride.riderId, ride.driverId, ride.pickupZone, km, Instant.now()))
                .get(10, TimeUnit.SECONDS).getRecordMetadata();
        return Map.of("rideId", rideId, "topic", md.topic(), "partition", md.partition(), "offset", md.offset());
    }

    @GetMapping("/{rideId}")
    public RideView get(@PathVariable String rideId) {
        return tracker.find(rideId).orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    }

    @GetMapping
    public Collection<RideView> all() {
        return tracker.all();
    }
}
