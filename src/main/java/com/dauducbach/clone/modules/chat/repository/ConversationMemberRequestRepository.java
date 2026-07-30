package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.entity.ConversationMemberRequest;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface ConversationMemberRequestRepository extends R2dbcRepository<ConversationMemberRequest, String> {

    @Query("SELECT * FROM conversation_member_requests WHERE id = :requestId FOR UPDATE")
    Mono<ConversationMemberRequest> findByIdForUpdate(String requestId);

    @Query("""
            SELECT * FROM conversation_member_requests
            WHERE conversation_id = :conversationId
              AND request_status = 'PENDING'
            ORDER BY created_at ASC, id ASC
            LIMIT :limit
            """)
    Flux<ConversationMemberRequest> findPendingByConversationId(String conversationId, int limit);
}
