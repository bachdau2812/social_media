package com.dauducbach.clone.modules.post.stories.policy;

import com.dauducbach.clone.modules.post.entity.story.UserStories;

import java.time.Duration;
import java.time.Instant;

public final class StoryExpiryPolicy {
    private static final Duration DEFAULT_LIFETIME = Duration.ofHours(24);

    private StoryExpiryPolicy() { }

    public static Instant expirationFrom(Instant createdAt) {
        return createdAt == null ? null : createdAt.plus(DEFAULT_LIFETIME);
    }

    public static Instant effectiveExpiry(UserStories story) {
        if (story == null) return null;
        return story.getExpiredAt() != null ? story.getExpiredAt() : expirationFrom(story.getCreatedAt());
    }

    public static boolean isActive(UserStories story, Instant now) {
        Instant expiry = effectiveExpiry(story);
        return expiry != null && expiry.isAfter(now);
    }

    public static boolean isApprovedAndActive(UserStories story, Instant now) {
        return story != null
                && "APPROVED".equalsIgnoreCase(story.getStatus())
                && isActive(story, now);
    }
}
