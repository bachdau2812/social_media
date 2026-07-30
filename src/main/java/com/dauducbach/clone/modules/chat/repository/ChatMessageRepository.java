package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

@Repository
public interface ChatMessageRepository extends R2dbcRepository<ChatMessage, String> {

    @Query("""
            SELECT * FROM messages
            WHERE sender_id = :senderId
              AND client_message_id = :clientMessageId
            LIMIT 1
            """)
    Mono<ChatMessage> findBySenderIdAndClientMessageId(String senderId, String clientMessageId);

    @Query("""
            SELECT * FROM messages
            WHERE conversation_id = :conversationId
              AND message_seq = :messageSequence
            LIMIT 1
            """)
    Mono<ChatMessage> findByConversationIdAndMessageSeq(String conversationId, long messageSequence);
}
