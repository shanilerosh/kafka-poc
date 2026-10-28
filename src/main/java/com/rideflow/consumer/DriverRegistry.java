package com.rideflow.consumer;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.DriverState;
import com.rideflow.domain.Events.DriverStatusChanged;

/**
 * COMPACTED TOPIC AS A TABLE.
 *
 * driver.status is log-compacted: Kafka keeps the latest record per driverId. On every startup we replay the
 * topic from offset 0 and rebuild an in-memory map of "who is where". No database needed: the topic IS the
 * source of truth and this map is just a cache (a "materialized view") of it.
 *
 * Note the @TopicPartition: we MANUALLY ASSIGN all partitions and seek to 0, instead of subscribing and
 * letting the group coordinator decide. Every instance of the app needs the FULL picture, so they must not
 * split the partitions among themselves.
 */
@Component
public class DriverRegistry {

    private static final Logger log = LoggerFactory.getLogger(DriverRegistry.class);

    private final Map<String, DriverStatusChanged> drivers = new ConcurrentHashMap<>();

    // driver.status has 3 partitions (see TopicsConfig): "0-2"
    @KafkaListener(
            id = "driver-registry",
            groupId = "driver-registry",
            topicPartitions = @TopicPartition(topic = Topics.DRIVER_STATUS,
                    partitionOffsets = @PartitionOffset(partition = "0-2", initialOffset = "0")))
    void onStatus(DriverStatusChanged status) {
        drivers.put(status.driverId(), status);
        log.info("[registry] driver {} is now {} in {}", status.driverId(), status.state(), status.zone());
    }

    /** Atomically pick an AVAILABLE driver in the zone and mark them busy locally so two rides can't get the same one. */
    public synchronized Optional<DriverStatusChanged> reserve(String zone) {
        return drivers.values().stream()
                .filter(d -> d.state() == DriverState.AVAILABLE && d.zone().equals(zone))
                .findFirst()
                .map(d -> {
                    var busy = new DriverStatusChanged(d.driverId(), d.zone(), DriverState.ON_TRIP, d.changedAt());
                    drivers.put(d.driverId(), busy);
                    return busy;
                });
    }

    public Collection<DriverStatusChanged> all() {
        return drivers.values();
    }
}
