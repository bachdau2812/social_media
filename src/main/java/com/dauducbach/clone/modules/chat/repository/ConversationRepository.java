package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.entity.Conversation;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
public interface ConversationRepository extends R2dbcRepository<Conversation, String> {

    @Query("SELECT * FROM conversations WHERE direct_key = :directKey LIMIT 1")
    Mono<Conversation> findByDirectKey(String directKey);

    @Query("SELECT * FROM conversations WHERE id = :conversationId FOR UPDATE")
    Mono<Conversation> findByIdForUpdate(String conversationId);

    @Modifying
    @Query("""
            UPDATE conversations
            SET last_message_seq = :messageSequence,
                last_message_id = :messageId,
                last_message_at = :messageAt,
                updated_at = :messageAt
            WHERE id = :conversationId
            """)
    Mono<Integer> updateMessageSummary(String conversationId, long messageSequence, String messageId,
                                       Instant messageAt);
    @Modifying
    @Query("""
            UPDATE conversations
            SET is_dissolved = TRUE,
                updated_at = :updatedAt
            WHERE id = :conversationId
              AND conversation_type = 'GROUP'
              AND is_dissolved = FALSE
            """)
    Mono<Integer> markDissolved(String conversationId, Instant updatedAt);
}