package com.dauducbach.clone.modules.post.stories.policy;

import com.dauducbach.clone.modules.post.entity.story.UserStories;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class StoryExpiryPolicyTest {
    @Test
    void defaultsExpiryToTwentyFourHoursWhenLegacyStoryHasNoExplicitExpiry() {
        Instant createdAt = Instant.parse("2026-10-07T00:00:00Z");
        UserStories story = UserStories.builder().createdAt(createdAt).build();

        assertThat(StoryExpiryPolicy.effectiveExpiry(story)).isEqualTo(createdAt.plusSeconds(86_400));
        assertThat(StoryExpiryPolicy.isActive(story, createdAt.plusSeconds(86_399))).isTrue();
        assertThat(StoryExpiryPolicy.isActive(story, createdAt.plusSeconds(86_400))).isFalse();
    }

    @Test
    void prefersStoredExpiryForStoriesWithExplicitExpiry() {
        Instant expiry = Instant.parse("2026-10-08T00:00:00Z");
        UserStories story = UserStories.builder()
                .status("APPROVED")
                .createdAt(Instant.parse("2026-10-01T00:00:00Z"))
                .expiredAt(expiry)
                .build();

        assertThat(StoryExpiryPolicy.effectiveExpiry(story)).isEqualTo(expiry);
        assertThat(StoryExpiryPolicy.isApprovedAndActive(story, expiry.minusNanos(1))).isTrue();
        assertThat(StoryExpiryPolicy.isApprovedAndActive(story, expiry)).isFalse();
    }
}
