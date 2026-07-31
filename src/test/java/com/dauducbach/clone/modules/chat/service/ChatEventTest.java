package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.ChatEventType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ChatEventTest {

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(Instant.class, (JsonSerializer<Instant>) (src, typeOfSrc, context) -> new JsonPrimitive(src.toString()))
            .registerTypeAdapter(Instant.class, (JsonDeserializer<Instant>) (json, typeOfT, context) -> Instant.parse(json.getAsString()))
            .create();

    @Test
    void messageCreatedUsesPersistedMessageAsEventContract() {
        Instant createdAt = Instant.parse("2026-07-24T00:00:00Z");
        ChatMessageResponse message = new ChatMessageResponse(
                "m1", "c1", 1L, "client-1", "u1", null, null,
                MessageType.TEXT, "hello", null, null, null, createdAt, null, false, null);

        ChatEvent event = ChatEvent.messageCreated(message, List.of("u2"));

        assertThat(event.type()).isEqualTo(ChatEventType.MESSAGE_CREATED);
        assertThat(event.eventId()).isNotBlank();
        assertThat(event.conversationId()).isEqualTo("c1");
        assertThat(event.actorId()).isEqualTo("u1");
        assertThat(event.entityId()).isEqualTo("m1");
        assertThat(event.occurredAt()).isEqualTo(createdAt.toString());
        assertThat(event.recipientIds()).containsExactly("u2");
        assertThat(event.message()).isEqualTo(message);
    }

    @Test
    void gsonRoundTripsChatEventEnvelopeUsedByTaskNine() {
        ChatEvent event = new ChatEvent(
                ChatEventType.MESSAGE_CREATED,
                UUID.randomUUID().toString(),
                "c1",
                "u1",
                "m1",
                null,
                "2026-07-24T00:00:00Z",
                List.of("u2"),
                null,
                null,
                null);

        String payload = gson.toJson(event);
        JsonObject restored = gson.fromJson(payload, JsonObject.class);

        assertThat(restored.get("type").getAsString()).isEqualTo(event.type().name());
        assertThat(restored.get("eventId").getAsString()).isEqualTo(event.eventId());
        assertThat(restored.get("conversationId").getAsString()).isEqualTo(event.conversationId());
        assertThat(restored.get("actorId").getAsString()).isEqualTo(event.actorId());
        assertThat(restored.get("entityId").getAsString()).isEqualTo(event.entityId());
        assertThat(restored.get("occurredAt").getAsString()).isEqualTo(event.occurredAt());
        assertThat(restored.getAsJsonArray("recipientIds").get(0).getAsString()).isEqualTo("u2");
    }
}
