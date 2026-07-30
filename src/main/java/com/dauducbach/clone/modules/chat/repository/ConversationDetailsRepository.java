package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.dto.response.ConversationMemberResponse;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
@RequiredArgsConstructor
public class ConversationDetailsRepository {

    private final DatabaseClient databaseClient;

    public Flux<ConversationMemberResponse> findActiveMembers(String conversationId, String actorId) {
        return databaseClient.sql("""
                        SELECT cm.user_id,
                               cm.nickname,
                               ud.username,
                               ud.full_name,
                               cm.member_role,
                               (SELECT COALESCE(NULLIF(avatar.secure_url, ''), avatar.url)
                                FROM media avatar
                                WHERE avatar.owner_id = cm.user_id
                                  AND avatar.owner_type = 'AVATAR'
                                ORDER BY avatar.created_at DESC
                                LIMIT 1) AS avatar_url
                        FROM conversation_members cm
                        LEFT JOIN user_details ud ON ud.user_id = cm.user_id
                        WHERE cm.conversation_id = :conversationId
                          AND cm.member_status = 'ACTIVE'
                        ORDER BY CASE WHEN cm.user_id = :actorId THEN 1 ELSE 0 END,
                                 cm.joined_at ASC,
                                 cm.user_id ASC
                        """)
                .bind("conversationId", conversationId)
                .bind("actorId", actorId)
                .map((row, metadata) -> mapMember(row))
                .all();
    }

    private ConversationMemberResponse mapMember(Row row) {
        String userId = row.get("user_id", String.class);
        String nickname = row.get("nickname", String.class);
        String fullName = row.get("full_name", String.class);
        String username = row.get("username", String.class);
        return new ConversationMemberResponse(
                userId,
                firstNonBlank(nickname, fullName, username, userId),
                username,
                fullName,
                nickname,
                row.get("avatar_url", String.class),
                MemberRole.valueOf(row.get("member_role", String.class))
        );
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "Member";
    }
}
