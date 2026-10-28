package com.rideflow.domain;

import java.time.Instant;

/**
 * Every message on Kafka is an "event": a fact that already happened (past tense).
 * They are plain records serialized to JSON.
 */
public final class Events {
    private Events() {}

    public record RideRequested(String rideId, String riderId, String pickupZone, String dropoffZone, Instant requestedAt) {}

    public record RideMatched(String rideId, String riderId, String driverId, String pickupZone, Instant matchedAt) {}

    public record RideCompleted(String rideId, String riderId, String driverId, String pickupZone, double distanceKm, Instant completedAt) {}

    public record PaymentCompleted(String rideId, String riderId, String driverId, double amount, double surgeMultiplier, Instant paidAt) {}

    /** High-volume, low-value telemetry: a driver's GPS ping. */
    public record DriverLocation(String driverId, String zone, double lat, double lon, Instant timestamp) {}

    /** Low-volume "latest state" event. Lives on a log-COMPACTED topic. */
    public record DriverStatusChanged(String driverId, String zone, DriverState state, Instant changedAt) {}

    /** Produced by Kafka Streams: demand in a zone during a time window. */
    public record SurgeUpdate(String zone, long requestCount, double multiplier, Instant windowStart, Instant windowEnd) {}

    public enum DriverState { AVAILABLE, ON_TRIP, OFFLINE }
}
