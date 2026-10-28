package com.rideflow.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.time.Instant;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.*;
import com.rideflow.producer.EventPublisher;

/**
 * CONSUMER GROUP "matching-service".
 *
 * ride.requested has 6 partitions. With concurrency=3 this app runs 3 consumer threads in the group, each
 * owning 2 partitions. Start a second copy of the app and Kafka rebalances: each instance gets ~3 partitions.
 * Scaling out = starting more instances (up to #partitions; more than that sit idle).
 *
 * It is both a CONSUMER (ride.requested) and a PRODUCER (ride.matched, driver.status): the classic
 * event-driven pipeline step.
 *
 * ERROR HANDLING: request a ride in a zone with no drivers (e.g. "NOWHERE"). We throw, the DefaultErrorHandler
 * retries 3x with backoff, then publishes the record to ride.requested.DLT. See DeadLetterMonitor.
 */
@Component
public class MatchingService {

    private static final Logger log = LoggerFactory.getLogger(MatchingService.class);

    private final DriverRegistry registry;
    private final EventPublisher publisher;

    public MatchingService(DriverRegistry registry, EventPublisher publisher) {
        this.registry = registry;
        this.publisher = publisher;
    }

    @KafkaListener(topics = Topics.RIDE_REQUESTED, groupId = "matching-service", concurrency = "3")
    public void onRideRequested(RideRequested ride,
                                @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                                @Header(KafkaHeaders.OFFSET) long offset) {
        log.info("[matching] got {} (partition={} offset={})", ride.rideId(), partition, offset);

        var driver = registry.reserve(ride.pickupZone())
                .orElseThrow(() -> new NoDriverAvailableException(ride.pickupZone()));

        publisher.driverStatus(driver.driverId(), driver.zone(), DriverState.ON_TRIP);
        publisher.rideMatched(new RideMatched(ride.rideId(), ride.riderId(), driver.driverId(), ride.pickupZone(), Instant.now()));
    }
}
