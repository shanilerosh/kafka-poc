package com.rideflow.streams;

import java.time.Duration;
import java.time.Instant;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.rideflow.config.KafkaJson;
import com.rideflow.config.Topics;
import com.rideflow.domain.Events.RideRequested;
import com.rideflow.domain.Events.SurgeUpdate;

/**
 * KAFKA STREAMS: stateful stream processing as a library inside your app (no separate cluster).
 *
 *   ride.requested ──groupBy(pickupZone)──┬─ count()                      ──► state store "zone-totals" (queryable via REST)
 *                                         └─ 30s tumbling window count() ──► surge.pricing topic
 *
 * Ideas to notice:
 *   KStream  = an endless log of events;   KTable / count() = the latest value per key, derived from a stream.
 *   groupBy on a NEW key forces a repartition (an internal topic) so that all records of a zone meet on one task.
 *   State lives in a local RocksDB store, backed up to a changelog topic in Kafka, so it survives crashes.
 *   exactly_once_v2 (application.yml) makes read-process-write atomic, like the transaction in BillingService.
 */
@Configuration
public class SurgePricingTopology {

    public static final String ZONE_TOTALS_STORE = "zone-totals";
    private static final long SURGE_THRESHOLD = 5;   // requests per window before prices start to rise

    @Autowired
    void buildPipeline(StreamsBuilder builder) {
        var rideSerde = new JsonSerde<>(RideRequested.class, KafkaJson.MAPPER);
        var surgeSerde = new JsonSerde<>(SurgeUpdate.class, KafkaJson.MAPPER);

        KGroupedStream<String, RideRequested> byZone = builder
                .stream(Topics.RIDE_REQUESTED, Consumed.with(Serdes.String(), rideSerde))
                .groupBy((rideId, ride) -> ride.pickupZone(), Grouped.with(Serdes.String(), rideSerde));

        // 1) Running total per zone, kept in a named, queryable state store.
        byZone.count(Materialized.as(ZONE_TOTALS_STORE));

        // 2) Demand per 30-second window -> multiplier -> publish to a topic others can consume.
        byZone.windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                .count(Materialized.as("zone-demand-30s"))
                .toStream()
                .map((window, count) -> KeyValue.pair(window.key(), new SurgeUpdate(
                        window.key(), count, multiplier(count),
                        Instant.ofEpochMilli(window.window().start()), Instant.ofEpochMilli(window.window().end()))))
                .to(Topics.SURGE_PRICING, Produced.with(Serdes.String(), surgeSerde));
    }

    private static double multiplier(long requests) {
        double m = 1.0 + Math.max(0, requests - SURGE_THRESHOLD) * 0.1;
        return Math.round(Math.min(m, 3.0) * 10) / 10.0;
    }
}
