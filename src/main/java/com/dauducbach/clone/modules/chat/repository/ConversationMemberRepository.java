package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface ConversationMemberRepository extends R2dbcRepository<ConversationMember, String> {

    @Query("""
            SELECT * FROM conversation_members
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            LIMIT 1
            """)
    Mono<ConversationMember> findActive(String conversationId, String userId);

    @Query("""
            SELECT * FROM conversation_members
            WHERE conversation_id = :conversationId
              AND user_id = :userId
            LIMIT 1
            FOR UPDATE
            """)
    Mono<ConversationMember> findMembershipForUpdate(String conversationId, String userId);

    @Query("""
            SELECT user_id FROM conversation_members
            WHERE conversation_id = :conversationId
              AND member_status = 'ACTIVE'
            """)
    Flux<String> findActiveUserIds(String conversationId);

    @Query("""
            SELECT user_id FROM conversation_members
            WHERE conversation_id = :conversationId
              AND member_status = 'ACTIVE'
              AND member_role = 'ADMIN'
            """)
    Flux<String> findActiveAdminUserIds(String conversationId);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET nickname = :nickname
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> updateNickname(String conversationId, String userId, String nickname);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET nickname = NULL
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> clearNickname(String conversationId, String userId);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET muted_until = :mutedUntil
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> updateMutedUntil(String conversationId, String userId, java.time.Instant mutedUntil);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET muted_until = NULL
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> clearMutedUntil(String conversationId, String userId);

    @Query("""
            SELECT COUNT(*) FROM conversation_members
            WHERE conversation_id = :conversationId
              AND member_status = 'ACTIVE'
              AND member_role = 'ADMIN'
            """)
    Mono<Long> countActiveAdmins(String conversationId);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET last_delivered_seq = GREATEST(last_delivered_seq, :sequence)
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> advanceDeliveredSequence(String conversationId, String userId, long sequence);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET last_delivered_seq = GREATEST(last_delivered_seq, :sequence),
                last_read_seq = GREATEST(last_read_seq, :sequence)
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> advanceDeliveredAndReadSequence(String conversationId, String userId, long sequence);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET member_status = 'LEFT',
                member_role = 'USER',
                last_deleted_message_seq = :sequence,
                last_delivered_seq = GREATEST(last_delivered_seq, :sequence),
                last_read_seq = GREATEST(last_read_seq, :sequence),
                left_at = :leftAt
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> markLeft(String conversationId, String userId, long sequence, java.time.Instant leftAt);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET member_status = 'REMOVED',
                member_role = 'USER',
                last_deleted_message_seq = :sequence,
                last_delivered_seq = GREATEST(last_delivered_seq, :sequence),
                last_read_seq = GREATEST(last_read_seq, :sequence),
                left_at = :leftAt
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> markRemoved(String conversationId, String userId, long sequence, java.time.Instant leftAt);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET member_status = 'ACTIVE',
                member_role = 'USER',
                joined_seq = :joinedSequence,
                last_delivered_seq = GREATEST(last_delivered_seq, :hiddenThroughSequence),
                last_read_seq = GREATEST(last_read_seq, :hiddenThroughSequence),
                joined_at = :joinedAt,
                left_at = NULL
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status IN ('LEFT', 'REMOVED')
            """)
    Mono<Integer> reactivateMember(
            String conversationId,
            String userId,
            long joinedSequence,
            long hiddenThroughSequence,
            java.time.Instant joinedAt);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET member_role = :role
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> updateRole(String conversationId, String userId, MemberRole role);

    @Modifying
    @Query("""
            UPDATE conversation_members
            SET last_deleted_message_seq = :sequence,
                last_delivered_seq = GREATEST(last_delivered_seq, :sequence),
                last_read_seq = GREATEST(last_read_seq, :sequence)
            WHERE conversation_id = :conversationId
              AND user_id = :userId
              AND member_status = 'ACTIVE'
            """)
    Mono<Integer> advanceDeleteBoundary(String conversationId, String userId, long sequence);
}
