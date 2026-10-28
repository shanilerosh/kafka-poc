package com.rideflow.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.DriverState;
import com.rideflow.domain.Events.RideCompleted;
import com.rideflow.producer.EventPublisher;

/** When a ride completes, the driver becomes AVAILABLE again (a new record on the compacted driver.status topic). */
@Component
public class DriverReleaseService {

    private final EventPublisher publisher;

    public DriverReleaseService(EventPublisher publisher) {
        this.publisher = publisher;
    }

    @KafkaListener(topics = Topics.RIDE_COMPLETED, groupId = "driver-release-service")
    void onRideCompleted(RideCompleted ride) {
        publisher.driverStatus(ride.driverId(), ride.pickupZone(), DriverState.AVAILABLE);
    }
}
