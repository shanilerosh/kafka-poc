package com.rideflow.producer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.rideflow.domain.Events.DriverLocation;
import com.rideflow.domain.Events.DriverState;

/** Pretends to be 45 drivers' phones: one AVAILABLE status at startup, then a GPS ping per driver every second. */
@Component
@ConditionalOnProperty(name = "rideflow.simulator.enabled", havingValue = "true")
public class DriverSimulator {

    public static final List<String> ZONES = List.of("DOWNTOWN", "AIRPORT", "SUBURBS");

    private record Phone(String driverId, String zone, double[] pos) {}

    private final EventPublisher publisher;
    private final List<Phone> phones = new ArrayList<>();

    public DriverSimulator(EventPublisher publisher) {
        this.publisher = publisher;
        for (int i = 1; i <= 45; i++) {
            String zone = ZONES.get((i - 1) % ZONES.size());
            phones.add(new Phone("driver-" + i, zone, new double[]{40.0 + i * 0.01, -74.0 - i * 0.01}));
        }
    }

    @EventListener(ApplicationReadyEvent.class)
    void goOnline() {
        phones.forEach(p -> publisher.driverStatus(p.driverId(), p.zone(), DriverState.AVAILABLE));
    }

    @Scheduled(fixedRate = 1000, initialDelay = 5000)
    void ping() {
        var rnd = ThreadLocalRandom.current();
        for (Phone p : phones) {
            p.pos()[0] += rnd.nextDouble(-0.0005, 0.0005);
            p.pos()[1] += rnd.nextDouble(-0.0005, 0.0005);
            publisher.driverLocation(new DriverLocation(p.driverId(), p.zone(), p.pos()[0], p.pos()[1], Instant.now()));
        }
    }
}
