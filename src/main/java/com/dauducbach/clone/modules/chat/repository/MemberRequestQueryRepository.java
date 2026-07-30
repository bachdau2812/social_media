package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.MemberRequestStatus;
import com.dauducbach.clone.modules.chat.dto.response.ChatUserSummaryResponse;
import com.dauducbach.clone.modules.chat.dto.response.MemberRequestResponse;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class MemberRequestQueryRepository {

    private final DatabaseClient databaseClient;

    public Flux<MemberRequestResponse> findPending(String conversationId, int limit, long offset) {
        return databaseClient.sql("""
                SELECT request.id, request.conversation_id, request.request_status, request.created_at,
                       request.requested_by, requester.username AS requester_username,
                       requester.full_name AS requester_full_name,
                       request.target_user_id, target.username AS target_username,
                       target.full_name AS target_full_name,
                       (SELECT COALESCE(NULLIF(media.secure_url, ''), media.url)
                        FROM media WHERE media.owner_id = request.requested_by
                          AND media.owner_type = 'AVATAR'
                        ORDER BY media.created_at DESC LIMIT 1) AS requester_avatar_url,
                       (SELECT COALESCE(NULLIF(media.secure_url, ''), media.url)
                        FROM media WHERE media.owner_id = request.target_user_id
                          AND media.owner_type = 'AVATAR'
                        ORDER BY media.created_at DESC LIMIT 1) AS target_avatar_url
                FROM conversation_member_requests request
                LEFT JOIN user_details requester ON requester.user_id = request.requested_by
                LEFT JOIN user_details target ON target.user_id = request.target_user_id
                WHERE request.conversation_id = :conversationId
                  AND request.request_status = 'PENDING'
                ORDER BY request.created_at DESC, request.id DESC
                LIMIT :limit OFFSET :offset
                """)
                .bind("conversationId", conversationId)
                .bind("limit", limit)
                .bind("offset", offset)
                .map((row, metadata) -> map(row))
                .all();
    }

    public Mono<Long> countPending(String conversationId) {
        return databaseClient.sql("""
                SELECT COUNT(*) AS total
                FROM conversation_member_requests
                WHERE conversation_id = :conversationId
                  AND request_status = 'PENDING'
                """)
                .bind("conversationId", conversationId)
                .map((row, metadata) -> ((Number) row.get("total")).longValue())
                .one()
                .defaultIfEmpty(0L);
    }

    private MemberRequestResponse map(Row row) {
        ChatUserSummaryResponse requester = user(row, "requested_by", "requester_username", "requester_full_name", "requester_avatar_url");
        ChatUserSummaryResponse target = user(row, "target_user_id", "target_username", "target_full_name", "target_avatar_url");
        return new MemberRequestResponse(
                row.get("id", String.class),
                row.get("conversation_id", String.class),
                MemberRequestStatus.valueOf(row.get("request_status", String.class)),
                requester,
                target,
                ChatReadRepository.toInstant(row.get("created_at")));
    }

    private ChatUserSummaryResponse user(Row row, String idColumn, String usernameColumn, String fullNameColumn, String avatarColumn) {
        String userId = row.get(idColumn, String.class);
        String username = row.get(usernameColumn, String.class);
        String fullName = row.get(fullNameColumn, String.class);
        return new ChatUserSummaryResponse(userId, firstNonBlank(fullName, username, userId), username, fullName, row.get(avatarColumn, String.class));
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value;
        }
        return "Member";
    }
}