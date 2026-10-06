package com.dauducbach.clone.modules.post.dto.event;

import com.google.gson.JsonObject;

import java.time.Instant;

public record PostInteractionEvent(String eventId, String postId, String actorId, String impressionId,
                                   boolean isClick, int viewTime, int score, Instant occurredAt) {
    public JsonObject toJson() {
        JsonObject json = new JsonObject();
        json.addProperty("schemaVersion", 1);
        json.addProperty("eventId", eventId);
        json.addProperty("postId", postId);
        json.addProperty("actorId", actorId);
        json.addProperty("impressionId", impressionId);
        json.addProperty("isClick", isClick);
        json.addProperty("viewTime", viewTime);
        json.addProperty("score", score);
        json.addProperty("occurredAt", occurredAt.toString());
        return json;
    }
}
