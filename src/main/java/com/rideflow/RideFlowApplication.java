package com.rideflow;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableKafkaStreams   // starts the Kafka Streams topology defined in SurgePricingTopology
@EnableScheduling     // used by the driver simulator
public class RideFlowApplication {
    public static void main(String[] args) {
        SpringApplication.run(RideFlowApplication.class, args);
    }
}
