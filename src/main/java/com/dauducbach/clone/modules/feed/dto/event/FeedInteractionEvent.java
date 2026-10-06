package com.dauducbach.clone.modules.feed.dto.event;

import com.google.gson.JsonObject;
import java.time.Instant;

public record FeedInteractionEvent(String eventId, String userId, String postId, String action,
                                   String sourceId, Instant occurredAt) {
    public FeedInteractionEvent {
        if (eventId == null || eventId.isBlank() || userId == null || userId.isBlank()
                || postId == null || postId.isBlank() || sourceId == null || sourceId.isBlank() || occurredAt == null
                || !("LIKE".equals(action) || "COMMENT".equals(action) || "REPOST".equals(action))) {
            throw new IllegalArgumentException("Invalid canonical interaction");
        }
    }

    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("eventId", eventId);
        json.addProperty("userId", userId);
        json.addProperty("postId", postId);
        json.addProperty("action", action);
        json.addProperty("sourceId", sourceId);
        json.addProperty("occurredAt", occurredAt.toString());
        return json;
    }

    public static FeedInteractionEvent fromJson(JsonObject json) {
        return new FeedInteractionEvent(json.get("eventId").getAsString(), json.get("userId").getAsString(),
                json.get("postId").getAsString(), json.get("action").getAsString(),
                json.get("sourceId").getAsString(), Instant.parse(json.get("occurredAt").getAsString()));
    }
}
