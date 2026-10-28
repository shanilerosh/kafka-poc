package com.rideflow.consumer;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import com.rideflow.config.Topics;

/**
 * DEAD LETTER TOPICS hold messages we could not process. The recoverer copies the original record and adds headers
 * explaining what went wrong and where it came from. In production you would alert on these and have a way to replay.
 * We read values as raw Strings so even garbage bytes (the poison pill) can be displayed.
 */
@Component
public class DeadLetterMonitor {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterMonitor.class);

    @KafkaListener(topics = {Topics.RIDE_REQUESTED_DLT, Topics.RIDE_COMPLETED_DLT}, groupId = "dlt-monitor", containerFactory = "stringListenerFactory")
    void onDeadLetter(ConsumerRecord<String, String> record) {
        log.error("[DLT] {} key={} originalTopic={} reason=\"{}\" payload={}",
                record.topic(), record.key(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                firstLine(header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)),
                record.value());
    }

    private static String header(ConsumerRecord<?, ?> r, String name) {
        Header h = r.headers().lastHeader(name);
        return h == null ? "?" : new String(h.value(), StandardCharsets.UTF_8);
    }

    private static String firstLine(String s) {
        int i = s.indexOf('\n');
        return i < 0 ? s : s.substring(0, i);
    }
}
