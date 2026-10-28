package com.rideflow.consumer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.DriverLocation;

/**
 * BATCH CONSUMER for a high-volume topic. One poll() returns up to max.poll.records messages and we handle
 * them together (one DB insert / one aggregation instead of N). Latency is traded for throughput.
 */
@Component
public class AnalyticsService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

    private final Map<String, AtomicLong> pingsPerZone = new ConcurrentHashMap<>();
    private final AtomicLong batches = new AtomicLong();

    @KafkaListener(topics = Topics.DRIVER_LOCATION, groupId = "analytics-service", containerFactory = "batchListenerFactory")
    void onLocations(List<DriverLocation> batch) {
        batches.incrementAndGet();
        batch.forEach(l -> pingsPerZone.computeIfAbsent(l.zone(), z -> new AtomicLong()).incrementAndGet());
    }

    @Scheduled(fixedRate = 15_000, initialDelay = 15_000)
    void report() {
        log.info("[analytics] GPS pings per zone so far {} (processed in {} batches)", pingsPerZone, batches.get());
    }
}
