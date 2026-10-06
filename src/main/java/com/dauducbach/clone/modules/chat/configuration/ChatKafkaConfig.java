package com.dauducbach.clone.modules.chat.configuration;

import com.dauducbach.clone.modules.chat.service.KafkaChatEventPublisher;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class ChatKafkaConfig {
    @Bean
    public NewTopic chatMessageMutationTopic(
            @Value("${chat.mutations.kafka.partitions:3}") int partitions,
            @Value("${chat.mutations.kafka.replicas:1}") int replicas) {
        return TopicBuilder.name(KafkaChatEventPublisher.MESSAGE_MUTATION_TOPIC).partitions(partitions).replicas(replicas).build();
    }
    @Bean
    public NewTopic chatMessageReactionChangedTopic(
            @Value("${chat.reactions.kafka.partitions:3}") int partitions,
            @Value("${chat.reactions.kafka.replicas:1}") int replicas) {
        return TopicBuilder.name(KafkaChatEventPublisher.MESSAGE_REACTION_CHANGED_TOPIC)
                .partitions(partitions).replicas(replicas).build();
    }
}
