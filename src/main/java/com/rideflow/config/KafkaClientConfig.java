package com.rideflow.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

/**
 * The wiring between Spring and the Kafka client library.
 *
 * Tunables like acks / idempotence / read_committed live in application.yml.
 * What lives here is what can't be expressed as a property: which serializer, which error handling,
 * which listener "flavour" (record / batch / transactional).
 */
@Configuration
public class KafkaClientConfig {

    // ───────────────────────────── PRODUCERS ─────────────────────────────

    /** The normal producer: key = String, value = JSON. Used for fire-and-forget event publishing. */
    @Bean
    @Primary
    ProducerFactory<String, Object> producerFactory(KafkaProperties props) {
        return new DefaultKafkaProducerFactory<>(
                props.buildProducerProperties(null), new StringSerializer(), new JsonSerializer<Object>(KafkaJson.MAPPER));
    }

    /** Bean name matters: @RetryableTopic looks up a template called "kafkaTemplate". */
    @Bean
    @Primary
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

    /**
     * TRANSACTIONS: a producer with a transactional.id prefix. Everything sent through it inside a
     * transaction (plus the consumer offsets, see transactionalListenerFactory) commits or aborts atomically.
     */
    @Bean
    ProducerFactory<String, Object> transactionalProducerFactory(KafkaProperties props) {
        var pf = new DefaultKafkaProducerFactory<String, Object>(
                props.buildProducerProperties(null), new StringSerializer(), new JsonSerializer<Object>(KafkaJson.MAPPER));
        pf.setTransactionIdPrefix("rideflow-tx-");
        return pf;
    }

    @Bean
    KafkaTemplate<String, Object> transactionalKafkaTemplate(
            @Qualifier("transactionalProducerFactory") ProducerFactory<String, Object> pf) {
        return new KafkaTemplate<>(pf);
    }

    @Bean
    KafkaTransactionManager<String, Object> kafkaTransactionManager(
            @Qualifier("transactionalProducerFactory") ProducerFactory<String, Object> pf) {
        return new KafkaTransactionManager<>(pf);
    }

    /** Sends raw bytes. Used to inject a "poison pill" and to forward undeserializable records to a DLT. */
    @Bean
    KafkaTemplate<String, byte[]> bytesKafkaTemplate(KafkaProperties props) {
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                props.buildProducerProperties(null), new StringSerializer(), new ByteArraySerializer()));
    }

    // ───────────────────────────── CONSUMERS ─────────────────────────────

    /**
     * Value deserializer = ErrorHandlingDeserializer wrapping JsonDeserializer.
     * Without the wrapper, a single malformed message makes the consumer throw on poll() forever
     * (the "poison pill" problem). With it, the failure is captured and handed to the error handler.
     * The JSON type comes from a "__TypeId__" header the producer wrote.
     */
    @Bean
    @Primary
    ConsumerFactory<String, Object> jsonConsumerFactory(KafkaProperties props) {
        var json = new JsonDeserializer<Object>(KafkaJson.MAPPER);
        json.addTrustedPackages(KafkaJson.DOMAIN_PACKAGE);   // security: only deserialize our own classes
        return new DefaultKafkaConsumerFactory<>(
                props.buildConsumerProperties(null), new StringDeserializer(), new ErrorHandlingDeserializer<>(json));
    }

    /** Reads values as raw text. Used by the dead-letter monitor so it can read ANY payload, even garbage. */
    @Bean
    ConsumerFactory<String, String> stringConsumerFactory(KafkaProperties props) {
        return new DefaultKafkaConsumerFactory<>(
                props.buildConsumerProperties(null), new StringDeserializer(), new StringDeserializer());
    }

    // ───────────────────────────── ERROR HANDLING ─────────────────────────────

    /**
     * BLOCKING RETRY then DEAD LETTER TOPIC:
     *   1. the listener throws
     *   2. retry up to 3 times with exponential backoff (500ms, 1s, 2s) — the partition is blocked meanwhile
     *   3. still failing? publish the record to "<topic>.DLT" and move on
     * Deserialization errors skip step 2: retrying can't fix bad bytes, so they go straight to the DLT.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, Object> jsonTemplate, KafkaTemplate<String, byte[]> bytesTemplate) {
        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);   // undeserializable records keep their original bytes
        templates.put(Object.class, jsonTemplate);
        // Same partition number as the source record, topic name "<original>.DLT" (we pin the name explicitly).
        var recoverer = new DeadLetterPublishingRecoverer(templates,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        var backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(500);
        backOff.setMultiplier(2.0);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    // ───────────────────────────── LISTENER CONTAINER FACTORIES ─────────────────────────────
    // A @KafkaListener picks one of these by name. Same consumer logic, different "flavour".

    /** Default flavour: one record at a time, offset committed after each successful record. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> jsonConsumerFactory, DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(jsonConsumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return factory;
    }

    /** Batch flavour: the listener receives a List of records per poll(). Great for high-volume streams. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> batchListenerFactory(
            ConsumerFactory<String, Object> jsonConsumerFactory, DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(jsonConsumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        factory.setBatchListener(true);
        return factory;
    }

    /** Raw-text flavour for reading dead letters. */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> stringListenerFactory(
            ConsumerFactory<String, String> stringConsumerFactory) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(stringConsumerFactory);
        return factory;
    }

    /**
     * EXACTLY-ONCE (consume → process → produce): the container opens a Kafka transaction around each
     * record. Messages the listener produces AND the consumed offset are committed together, or neither is.
     * If the listener throws, the transaction aborts, the offset is NOT committed, and the record is redelivered.
     * After 2 retries the record goes to the DLT.
     */
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> transactionalListenerFactory(
            ConsumerFactory<String, Object> jsonConsumerFactory,
            KafkaTransactionManager<String, Object> transactionManager,
            @Qualifier("transactionalKafkaTemplate") KafkaTemplate<String, Object> transactionalTemplate) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(jsonConsumerFactory);
        factory.getContainerProperties().setKafkaAwareTransactionManager(transactionManager);
        factory.setAfterRollbackProcessor(new DefaultAfterRollbackProcessor<String, Object>(
                new DeadLetterPublishingRecoverer(transactionalTemplate,
                        (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition())), new FixedBackOff(1000L, 2L), transactionalTemplate, true));
        return factory;
    }
}
