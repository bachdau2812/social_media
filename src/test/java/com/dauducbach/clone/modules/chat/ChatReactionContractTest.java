package com.dauducbach.clone.modules.chat;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import org.junit.jupiter.api.Test;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import static org.assertj.core.api.Assertions.assertThat;

class ChatReactionContractTest {
    @Test
    void legacyMessageConstructorHasExactNeutralJsonDefaults() throws Exception {
        var message = new ChatMessageResponse("m", "c", 5, "client", "me", null, null,
                com.dauducbach.clone.modules.chat.constant.MessageType.TEXT, "hello", null,
                null, null, null, null, false, null);
        var json = new com.fasterxml.jackson.databind.ObjectMapper().valueToTree(message);
        assertThat(json.get("likeCount").asLong()).isZero();
        assertThat(json.get("isReact").asBoolean()).isFalse();
        assertThat(json.get("myReaction").isNull()).isTrue();
        assertThat(json.get("reactions").isArray()).isTrue();
        assertThat(json.get("reactions").size()).isZero();
        assertThat(json.get("reactionVersion").asLong()).isZero();
    }
    @Test
    void messageResponsesExposePersonalReactionAndRevision() {
        assertThat(Arrays.stream(ChatMessageResponse.class.getRecordComponents()).map(c -> c.getName()))
                .contains("likeCount", "isReact", "myReaction", "reactions", "reactionVersion");
    }

    @Test
    void broadcastCarriesNeutralReactionState() {
        assertThat(Arrays.stream(ChatEvent.class.getRecordComponents()).map(c -> c.getName()))
                .contains("reactionState");
    }

    @Test
    void migrationEnforcesOneReactionPerActorAndOwnDurableOutbox() throws Exception {
        Path migration = Path.of("src/main/resources/db/manual/chat_reactions_schema.sql");
        assertThat(migration).exists();
        assertThat(Files.readString(migration)).contains("PRIMARY KEY (message_id, user_id)",
                "reaction_version", "chat_reaction_outbox", "'HEART', 'LIKE', 'HAHA', 'WOW', 'SAD', 'ANGRY'");
    }
}
