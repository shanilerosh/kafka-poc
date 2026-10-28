package com.rideflow.streams;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

/** INTERACTIVE QUERIES: read Streams' local state store directly, no database in between. */
@Component
public class ZoneDemandQuery {

    private final StreamsBuilderFactoryBean streams;

    public ZoneDemandQuery(StreamsBuilderFactoryBean streams) {
        this.streams = streams;
    }

    public Map<String, Long> totalsPerZone() {
        KafkaStreams ks = streams.getKafkaStreams();
        if (ks == null || ks.state() != KafkaStreams.State.RUNNING) {
            throw new IllegalStateException("Kafka Streams is not RUNNING yet (state=" + (ks == null ? "n/a" : ks.state()) + ")");
        }
        ReadOnlyKeyValueStore<String, Long> store = ks.store(
                StoreQueryParameters.fromNameAndType(SurgePricingTopology.ZONE_TOTALS_STORE, QueryableStoreTypes.keyValueStore()));
        Map<String, Long> out = new LinkedHashMap<>();
        store.all().forEachRemaining(kv -> out.put(kv.key, kv.value));
        return out;
    }
}
