package com.dauducbach.clone.modules.streams.post;

import com.dauducbach.clone.modules.post.dto.event.PostEngagementEvent;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Clock;
import java.time.Instant;

/** Raw schema boundaries are kept outside the stateful graph. */
public final class EngagementNormalizer {
    public record Parsed(PostEngagementEvent event, String error, String raw) {}
    private final Clock clock;

    public EngagementNormalizer(Clock clock) { this.clock = clock; }

    public Parsed parse(String kind, String key, String raw) {
        try {
            JsonObject json = JsonParser.parseString(raw).getAsJsonObject();
            if (kind.equals("LIKE") && !"POST".equals(text(json, "targetType"))) return new Parsed(null, null, raw);
            String id = text(json, "eventId");
            String post = text(json, "postId");
            String actor = text(json, kind.equals("COMMENT") ? "userId" : "actorId");
            Instant time = timestamp(json);
            if (time.isBefore(Instant.EPOCH) || time.isAfter(clock.instant().plusSeconds(60)))
                throw new IllegalArgumentException("invalid occurrence time");
            String impression = "";
            boolean click = false;
            int view = 0;
            long weight = 1;
            if (kind.equals("VIEW")) {
                if (!post.equals(key) || json.get("schemaVersion").getAsInt() != 1)
                    throw new IllegalArgumentException("interaction key/schema mismatch");
                impression = text(json, "impressionId");
                if (!json.get("isClick").isJsonPrimitive() || !json.get("isClick").getAsJsonPrimitive().isBoolean())
                    throw new IllegalArgumentException("invalid click");
                click = json.get("isClick").getAsBoolean();
                java.math.BigDecimal seconds = json.get("viewTime").getAsBigDecimal();
                view = seconds.intValueExact();
                if (view < 0 || view > 3600) throw new IllegalArgumentException("invalid dwell");
                weight = 0;
            }
            return new Parsed(new PostEngagementEvent(id, post, actor, kind, impression, click, view, weight, time), null, raw);
        } catch (RuntimeException error) {
            return new Parsed(null, "Invalid " + kind + " event", raw);
        }
    }

    private static String text(JsonObject json, String field) {
        if (!json.has(field) || json.get(field).isJsonNull()) throw new IllegalArgumentException("missing " + field);
        String value = json.get(field).getAsString();
        if (value.isBlank() || value.length() > 512) throw new IllegalArgumentException("invalid " + field);
        return value;
    }

    private static Instant timestamp(JsonObject json) {
        return Instant.parse(text(json, json.has("popularityOccurredAt") ? "popularityOccurredAt" : "occurredAt"));
    }

    /** Invalid raw timestamps must not advance task stream-time before quarantine. */
    public TimestampExtractor extractor(String kind) {
        return (ConsumerRecord<Object, Object> record, long partitionTime) -> {
            try {
                Parsed parsed = parse(kind, (String) record.key(), (String) record.value());
                if (parsed.event() != null) return parsed.event().occurredAt().toEpochMilli();
            } catch (RuntimeException ignored) { }
            return Math.max(0, partitionTime);
        };
    }
}
