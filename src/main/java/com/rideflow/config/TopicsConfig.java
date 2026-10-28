package com.rideflow.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

/**
 * TOPICS, PARTITIONS, REPLICATION.
 *
 * Spring's KafkaAdmin creates these on startup if they don't exist.
 * Each topic is a decision about three things:
 *   partitions  -> max parallelism for consumers in one group (and the unit of ordering)
 *   replicas    -> copies of each partition on different brokers (fault tolerance)
 *   configs     -> retention / compaction / min in-sync replicas
 */
@Configuration
public class TopicsConfig {

    private static final int BROKERS = 3;

    @Bean
    KafkaAdmin.NewTopics rideFlowTopics() {
        return new KafkaAdmin.NewTopics(
                // Business events: 6 partitions => up to 6 parallel consumers per group. Kept 7 days.
                durable(Topics.RIDE_REQUESTED, 6).build(),
                durable(Topics.RIDE_MATCHED, 6).build(),
                durable(Topics.RIDE_COMPLETED, 6).build(),
                durable(Topics.PAYMENT_COMPLETED, 6).build(),

                // Telemetry: huge volume, worthless after an hour => short retention, delete old segments.
                durable(Topics.DRIVER_LOCATION, 6)
                        .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(60 * 60 * 1000L))
                        .build(),

                // LOG COMPACTION: Kafka keeps only the LATEST message per key (driverId), like a table.
                // The aggressive timings below exist only so you can watch compaction happen in a demo.
                durable(Topics.DRIVER_STATUS, 3)
                        .config(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
                        .config(TopicConfig.SEGMENT_MS_CONFIG, "10000")
                        .config(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.01")
                        .config(TopicConfig.DELETE_RETENTION_MS_CONFIG, "1000")
                        .build(),

                // Output of Kafka Streams, keyed by zone.
                durable(Topics.SURGE_PRICING, 3).build(),

                // Dead letter topics need >= as many partitions as the source topic, because the
                // default recoverer publishes to the SAME partition number.
                durable(Topics.RIDE_REQUESTED_DLT, 6).build(),
                durable(Topics.RIDE_COMPLETED_DLT, 6).build()
        );
    }

    private TopicBuilder durable(String name, int partitions) {
        return TopicBuilder.name(name)
                .partitions(partitions)
                .replicas(BROKERS)
                // With acks=all, a write succeeds only when at least 2 replicas have it.
                // => we survive 1 broker dying with zero data loss, and refuse writes if 2 die.
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
    }
}
