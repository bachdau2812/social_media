package com.dauducbach.clone.modules.post.popularity.infrastructure.streams;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@ConditionalOnProperty(name = "post.popularity.streams-enabled", havingValue = "true")
public class PostPopularityStreamsConfig {
    @Bean(initMethod = "start", destroyMethod = "close")
    public KafkaStreams postPopularityStreams(PostPopularityProperties properties, KafkaProperties kafka) {
        Properties config = new Properties();
        config.putAll(kafka.buildStreamsProperties(null));
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, properties.getApplicationId());
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        KafkaStreams streams = new KafkaStreams(PostPopularityTopology.build(properties), config);
        streams.setUncaughtExceptionHandler(error -> StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION);
        return streams;
    }
}
