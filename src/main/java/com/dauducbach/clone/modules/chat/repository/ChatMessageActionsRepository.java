package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class ChatMessageActionsRepository {
    public static final String RECALL_SQL = """
            UPDATE messages
            SET deleted_at = :at, content = NULL, metadata = NULL, reply_to_seq = NULL,
                reaction_version = reaction_version + 1
            WHERE id = :id AND deleted_at IS NULL
            """;
    public static final String REFERENCE_SQL = """
            INSERT INTO chat_message_media_refs(message_id, asset_id)
            SELECT :id, asset_id FROM media WHERE owner_id = :source AND owner_type = 'CHAT_MESSAGE'
            UNION SELECT :id, asset_id FROM chat_message_media_refs WHERE message_id = :source
            """;

    private final DatabaseClient db;

    public Mono<ChatMessage> lockMessage(String conversationId, String messageId) {
        return db.sql("SELECT * FROM messages WHERE conversation_id = :conversationId AND id = :id FOR UPDATE")
                .bind("conversationId", conversationId).bind("id", messageId)
                .map((row, metadata) -> ChatMessage.builder()
                        .id(row.get("id", String.class))
                        .conversationId(row.get("conversation_id", String.class))
                        .messageSeq(((Number) row.get("message_seq")).longValue())
                        .senderId(row.get("sender_id", String.class))
                        .clientMessageId(row.get("client_message_id", String.class))
                        .messageType(MessageType.valueOf(row.get("message_type", String.class)))
                        .content(row.get("content", String.class))
                        .metadata(row.get("metadata", String.class))
                        .replyToSeq(row.get("reply_to_seq", Long.class))
                        .createdAt(ChatReadRepository.toInstant(row.get("created_at")))
                        .editedAt(ChatReadRepository.toInstant(row.get("edited_at")))
                        .deletedAt(ChatReadRepository.toInstant(row.get("deleted_at")))
                        .forwarded(Boolean.TRUE.equals(row.get("forwarded", Boolean.class)))
                        .reactionVersion(((Number) row.get("reaction_version")).longValue())
                        .build())
                .one();
    }

    public Mono<Void> recall(String messageId, Instant recalledAt) {
        return db.sql(RECALL_SQL).bind("id", messageId).bind("at", recalledAt).fetch().rowsUpdated().then();
    }

    public Mono<Void> removeReactions(String messageId) {
        return db.sql("DELETE FROM message_reactions WHERE message_id = :id")
                .bind("id", messageId).fetch().rowsUpdated().then();
    }

    public Mono<Long> pinVersion(String conversationId) {
        return db.sql("SELECT pin_version FROM conversations WHERE id = :conversationId")
                .bind("conversationId", conversationId)
                .map((row, metadata) -> ((Number) row.get("pin_version")).longValue()).one();
    }

    public Mono<Void> incrementPinVersion(String conversationId) {
        return db.sql("UPDATE conversations SET pin_version = pin_version + 1 WHERE id = :conversationId")
                .bind("conversationId", conversationId).fetch().rowsUpdated().then();
    }

    public Mono<Long> pinCount(String conversationId) {
        return db.sql("SELECT COUNT(*) AS count FROM chat_message_pins WHERE conversation_id = :conversationId")
                .bind("conversationId", conversationId)
                .map((row, metadata) -> ((Number) row.get("count")).longValue()).one();
    }

    public Mono<Boolean> hasPin(String conversationId, String messageId) {
        return db.sql("""
                        SELECT message_id FROM chat_message_pins
                        WHERE conversation_id = :conversationId AND message_id = :messageId
                        """)
                .bind("conversationId", conversationId).bind("messageId", messageId)
                .map((row, metadata) -> true).one().defaultIfEmpty(false);
    }

    public Mono<Void> addPin(String conversationId, String messageId, String actorId, Instant pinnedAt) {
        return db.sql("""
                        INSERT INTO chat_message_pins(conversation_id, message_id, pinned_by, pinned_at)
                        VALUES (:conversationId, :messageId, :actorId, :pinnedAt)
                        """)
                .bind("conversationId", conversationId).bind("messageId", messageId)
                .bind("actorId", actorId).bind("pinnedAt", pinnedAt).fetch().rowsUpdated().then();
    }

    public Mono<Long> removePin(String conversationId, String messageId) {
        return db.sql("""
                        DELETE FROM chat_message_pins
                        WHERE conversation_id = :conversationId AND message_id = :messageId
                        """)
                .bind("conversationId", conversationId).bind("messageId", messageId).fetch().rowsUpdated();
    }

    public Flux<PinRow> pins(String conversationId) {
        return db.sql("""
                        SELECT pin.message_id, pin.pinned_by, pin.pinned_at, message.message_seq
                        FROM chat_message_pins pin
                        JOIN messages message
                          ON message.id = pin.message_id AND message.conversation_id = pin.conversation_id
                        WHERE pin.conversation_id = :conversationId AND message.deleted_at IS NULL
                        ORDER BY pin.pinned_at DESC, pin.message_id ASC
                        """)
                .bind("conversationId", conversationId)
                .map((row, metadata) -> new PinRow(row.get("message_id", String.class),
                        row.get("pinned_by", String.class), ChatReadRepository.toInstant(row.get("pinned_at")),
                        ((Number) row.get("message_seq")).longValue())).all();
    }

    public Mono<Void> recordForward(String messageId, String sourceConversationId, String sourceMessageId) {
        return db.sql("""
                        INSERT INTO chat_message_forwards(message_id, source_conversation_id, source_message_id)
                        VALUES (:messageId, :sourceConversationId, :sourceMessageId)
                        """)
                .bind("messageId", messageId).bind("sourceConversationId", sourceConversationId)
                .bind("sourceMessageId", sourceMessageId).fetch().rowsUpdated().then();
    }

    public Mono<Boolean> matchesForward(String messageId, String sourceConversationId, String sourceMessageId) {
        return db.sql("""
                        SELECT message_id FROM chat_message_forwards
                        WHERE message_id = :messageId AND source_conversation_id = :sourceConversationId
                          AND source_message_id = :sourceMessageId
                        """)
                .bind("messageId", messageId).bind("sourceConversationId", sourceConversationId)
                .bind("sourceMessageId", sourceMessageId).map((row, metadata) -> true).one().defaultIfEmpty(false);
    }

    public Mono<Long> referenceMedia(String messageId, String sourceMessageId) {
        return db.sql(REFERENCE_SQL).bind("id", messageId).bind("source", sourceMessageId).fetch().rowsUpdated();
    }

    public record PinRow(String messageId, String pinnedBy, Instant pinnedAt, long messageSeq) {}
}
