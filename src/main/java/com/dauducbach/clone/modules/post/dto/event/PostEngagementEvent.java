package com.dauducbach.clone.modules.post.dto.event;

import java.time.Instant;

/** Canonical facts used only by the popularity pipeline. */
public record PostEngagementEvent(String eventId, String postId, String actorId, String kind,
                                  String impressionId, boolean clicked, int viewTime, long weight,
                                  Instant occurredAt) {
    public PostEngagementEvent withWeight(long delta) {
        return new PostEngagementEvent(eventId, postId, actorId, kind, impressionId,
                clicked, viewTime, delta, occurredAt);
    }
}
