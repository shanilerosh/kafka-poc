package com.rideflow.api;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.RideRequested;
import com.rideflow.producer.EventPublisher;

/** Buttons for experiments. */
@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final EventPublisher publisher;
    private final KafkaTemplate<String, byte[]> bytesTemplate;

    public DemoController(EventPublisher publisher, KafkaTemplate<String, byte[]> bytesTemplate) {
        this.publisher = publisher;
        this.bytesTemplate = bytesTemplate;
    }

    /**
     * Fire N ride requests at once in a zone => demand spike => Kafka Streams raises the surge multiplier.
     * Each zone has 15 drivers: ask for more than 15 and the extra rides can't be matched => retries => DLT.
     */
    @PostMapping("/burst")
    public Map<String, Object> burst(@RequestParam(defaultValue = "DOWNTOWN") String zone, @RequestParam(defaultValue = "12") int count) {
        for (int i = 0; i < count; i++) {
            String id = "ride-" + UUID.randomUUID().toString().substring(0, 8);
            publisher.rideRequested(new RideRequested(id, "rider-burst-" + i, zone, "AIRPORT", Instant.now()));
        }
        return Map.of("published", count, "zone", zone);
    }

    /** POISON PILL: bytes that are not valid JSON, written straight onto ride.requested. */
    @PostMapping("/poison-pill")
    public Map<String, String> poisonPill() {
        bytesTemplate.send(Topics.RIDE_REQUESTED, "poison-" + UUID.randomUUID(), "this is {not json".getBytes(UTF_8));
        return Map.of("sent", "garbage bytes to " + Topics.RIDE_REQUESTED, "expect", "a record in " + Topics.RIDE_REQUESTED_DLT + ", app keeps running");
    }
}
