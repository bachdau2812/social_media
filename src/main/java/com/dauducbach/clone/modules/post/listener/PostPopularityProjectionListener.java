package com.dauducbach.clone.modules.post.listener;

import com.dauducbach.clone.modules.post.dto.event.PostEventJson;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import com.dauducbach.clone.modules.post.service.post.PostPopularityProjectionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Objects;

@Component
@ConditionalOnProperty(name = "post.popularity.projection-enabled", havingValue = "true")
public class PostPopularityProjectionListener {
    private static final Duration PROJECTION_TIMEOUT = Duration.ofSeconds(30);
    private final PostPopularityProjectionService projection;

    public PostPopularityProjectionListener(PostPopularityProjectionService projection) {
        this.projection = projection;
    }

    @KafkaListener(topics = "${post.popularity.output-topic:post_popularity_updates}",
            groupId = "${post.popularity.projection-group-id:post-popularity-projection-v1}",
            containerFactory = "postPopularityListenerFactory")
    public void receive(ConsumerRecord<String, String> record) {
        // Compacted-topic tombstones are deletion metadata, not a new promotion.
        if (record.value() == null) {
            return;
        }
        var update = PostEventJson.read(record.value(), PostPopularityUpdate.class);
        if (!Objects.equals(record.key(), update.postId())) {
            throw new IllegalArgumentException("Popularity key mismatch");
        }
        // Intentionally wait only on the dedicated Kafka consumer thread, never a WebFlux thread.
        // Throwing here lets the container retry or durably dead-letter before committing the offset.
        projection.apply(update).block(PROJECTION_TIMEOUT);
    }
}
