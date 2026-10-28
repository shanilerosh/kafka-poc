package com.rideflow.consumer;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;
import com.rideflow.domain.Events.SurgeUpdate;

/** Reads the output of the Kafka Streams job (surge.pricing) and remembers the latest multiplier per zone. */
@Component
public class SurgeCache {

    private static final Logger log = LoggerFactory.getLogger(SurgeCache.class);

    private final Map<String, SurgeUpdate> latest = new ConcurrentHashMap<>();

    @KafkaListener(topics = Topics.SURGE_PRICING, groupId = "surge-cache")
    void onSurge(SurgeUpdate update) {
        latest.put(update.zone(), update);
        log.info("[surge] {} had {} requests in window ending {} -> x{}", update.zone(), update.requestCount(), update.windowEnd(), update.multiplier());
    }

    /** Surge only counts while its window is recent; afterwards prices go back to normal. */
    public double multiplierFor(String zone) {
        var u = latest.get(zone);
        if (u == null || u.windowEnd().isBefore(Instant.now().minusSeconds(60))) {
            return 1.0;
        }
        return u.multiplier();
    }

    public Map<String, SurgeUpdate> all() {
        return latest;
    }
}
