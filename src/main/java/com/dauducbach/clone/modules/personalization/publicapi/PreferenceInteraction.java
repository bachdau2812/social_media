package com.dauducbach.clone.modules.personalization.publicapi;

import java.time.Instant;

/** Immutable feed interaction input used to update a user's preference state. */
public record PreferenceInteraction(String eventId, String userId, String postId, String action,
                                    String sourceId, Instant occurredAt) {
    public PreferenceInteraction {
        if (eventId == null || eventId.isBlank() || userId == null || userId.isBlank()
                || postId == null || postId.isBlank() || sourceId == null || sourceId.isBlank()
                || occurredAt == null || !("LIKE".equals(action) || "COMMENT".equals(action) || "REPOST".equals(action)))
            throw new IllegalArgumentException("Invalid preference interaction");
    }
}
