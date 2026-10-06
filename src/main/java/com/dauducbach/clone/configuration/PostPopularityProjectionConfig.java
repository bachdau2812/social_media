package com.dauducbach.clone.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;

@Configuration
@ConditionalOnProperty(name = "post.popularity.projection-enabled", havingValue = "true")
public class PostPopularityProjectionConfig {
    @Bean
    public DefaultKafkaProducerFactory<String, String> postPopularityDeadLetterProducerFactory(KafkaProperties kafka) {
        var producer = new HashMap<String, Object>(kafka.buildProducerProperties(null));
        producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producer.put(ProducerConfig.ACKS_CONFIG, "all");
        producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(producer);
    }

    @Bean
    public KafkaTemplate<String, String> postPopularityDeadLetterTemplate(
            @Qualifier("postPopularityDeadLetterProducerFactory") DefaultKafkaProducerFactory<String, String> producers) {
        return new KafkaTemplate<>(producers);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> postPopularityListenerFactory(
            KafkaProperties kafka, @Qualifier("postPopularityDeadLetterTemplate") KafkaTemplate<String, String> template) {
        var consumer = new HashMap<String, Object>(kafka.buildConsumerProperties(null));
        consumer.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumer.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        consumer.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumer.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // One bounded projection per poll keeps the Kafka consumer responsive during Redis failure.
        consumer.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, error) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        recoverer.setFailIfSendResultIsError(true);
        var errors = new DefaultErrorHandler(recoverer, new FixedBackOff(1000, 5));
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumer));
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(errors);
        return factory;
    }
}
