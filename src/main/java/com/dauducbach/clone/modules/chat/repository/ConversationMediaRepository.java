package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.ChatMediaCategory;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class ConversationMediaRepository {

    private final DatabaseClient databaseClient;

    public Flux<ConversationMediaRow> findMedia(
            String conversationId,
            long joinedSeq,
            ChatMediaCategory category,
            long beforeSeq,
            int limit
    ) {
        String typePredicate = switch (category) {
            case IMAGE -> "message.message_type = 'IMAGE'";
            case VIDEO -> "message.message_type = 'VIDEO'";
            case FILE_AUDIO -> "message.message_type IN ('FILE', 'AUDIO')";
        };
        String sql = """
                SELECT message.id, message.message_seq, message.message_type,
                       message.metadata, message.created_at
                FROM messages message
                WHERE message.conversation_id = :conversationId
                  AND message.message_seq >= :joinedSeq
                  AND message.message_seq < :beforeSeq
                  AND message.deleted_at IS NULL
                  AND message.metadata IS NOT NULL
                  AND %s
                ORDER BY message.message_seq DESC
                LIMIT :limit
                """.formatted(typePredicate);
        return databaseClient.sql(sql)
                .bind("conversationId", conversationId)
                .bind("joinedSeq", joinedSeq)
                .bind("beforeSeq", beforeSeq)
                .bind("limit", limit)
                .map((row, metadata) -> map(row))
                .all();
    }

    private ConversationMediaRow map(Row row) {
        return new ConversationMediaRow(
                row.get("id", String.class),
                ((Number) row.get("message_seq")).longValue(),
                MessageType.valueOf(row.get("message_type", String.class)),
                row.get("metadata", String.class),
                ChatReadRepository.toInstant(row.get("created_at")));
    }

    public record ConversationMediaRow(
            String messageId,
            long messageSeq,
            MessageType messageType,
            String metadata,
            Instant createdAt
    ) {
    }
}