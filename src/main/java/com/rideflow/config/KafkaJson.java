package com.rideflow.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.json.JsonMapper;

/** One ObjectMapper for every JSON (de)serializer, so timestamps are readable ISO strings in Kafka UI. */
public final class KafkaJson {
    private KafkaJson() {}

    public static final String DOMAIN_PACKAGE = "com.rideflow.domain";

    public static final ObjectMapper MAPPER = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .build();
}
