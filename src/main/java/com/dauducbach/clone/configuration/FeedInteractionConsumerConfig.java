package com.dauducbach.clone.configuration;

import com.dauducbach.clone.modules.feed.constant.FeedTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@ConditionalOnProperty(name = "vector.interaction.consumer.enabled", havingValue = "true")
public class FeedInteractionConsumerConfig {
    @Bean("feedInteractionReceiverOptions")
    public ReceiverOptions<String, String> feedInteractionReceiverOptions(
            @Value("${spring.kafka.bootstrap-servers:127.0.0.1:9092}") String bootstrapServers,
            @Value("${vector.interaction.consumer.group-id:feed-service}") String groupId,
            @Value("${vector.interaction.consumer.partition-count:1}") int partitionCount) {
        if (partitionCount < 1) throw new IllegalArgumentException("Canonical partition count must be positive and fixed");
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, partitionCount);
        return ReceiverOptions.<String, String>create(properties)
                .subscription(List.of(FeedTopics.USER_INTERACTION_EVENTS))
                // Only explicit per-record commit after durable apply; no interval/batch acknowledgement.
                .commitInterval(Duration.ZERO).commitBatchSize(0)
                .maxDeferredCommits(0)
                .addAssignListener(partitions -> partitions.forEach(partition -> {
                    if (partition.topicPartition().partition() >= partitionCount)
                        throw new IllegalStateException("Canonical partition topology changed; migrate generation/cursors before rollout");
                }));
    }
}
