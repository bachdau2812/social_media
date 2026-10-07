package com.dauducbach.clone.modules.post.infrastructure.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration(proxyBeanMethods = false)
public class PostMediaScanKafkaConfiguration {
    @Bean
    public NewTopic postMediaScanTopic(@Value("${post.media.scan.topic:check_media_event}") String topic) {
        // Use broker defaults for partitions/replicas; do not rely on producer auto-creation.
        return TopicBuilder.name(topic).build();
    }
}
