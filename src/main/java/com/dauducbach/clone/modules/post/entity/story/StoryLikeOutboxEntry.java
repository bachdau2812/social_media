package com.dauducbach.clone.modules.post.entity.story;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Table("story_like_outbox")
public class StoryLikeOutboxEntry {
    @Id
    String interactionId;
    String storyId;
    String actorId;
    String ownerId;
    Instant createdAt;
    int attemptCount;
    Instant nextAttemptAt;
    String leaseToken;
    Instant leaseUntil;
}
