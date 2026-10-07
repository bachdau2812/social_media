package com.dauducbach.clone.modules.feed.listener;

import com.dauducbach.clone.modules.feed.dto.event.FeedInteractionEvent;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteraction;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class FeedInteractionConsumerMappingTest {
    @Test
    void mapsTransportEventToPersonalizationInputWithoutChangingIdentity() {
        Instant occurredAt = Instant.parse("2026-10-01T12:30:00Z");
        FeedInteractionEvent source = new FeedInteractionEvent(
                "event-1", "user-1", "post-1", "COMMENT", "comment-1", occurredAt);

        PreferenceInteraction mapped = FeedInteractionConsumer.toPreferenceInteraction(source);

        assertThat(mapped).isEqualTo(new PreferenceInteraction(
                "event-1", "user-1", "post-1", "COMMENT", "comment-1", occurredAt));
    }
}
