package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.dto.response.ChatUserSuggestionResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
@RequiredArgsConstructor
public class ChatUserSuggestionRepository {
    private final DatabaseClient databaseClient;

    public Flux<ChatUserSuggestionResponse> findSuggestions(String viewerId, String queryPattern, int limit) {
        boolean emptyQuery = queryPattern == null || queryPattern.isBlank();
        return databaseClient.sql("""
                        SELECT candidate.user_id, candidate.username, candidate.full_name,
                               (SELECT COALESCE(NULLIF(avatar.secure_url, ''), avatar.url)
                                FROM media avatar
                                WHERE avatar.owner_id = candidate.user_id AND avatar.owner_type = 'AVATAR'
                                ORDER BY avatar.created_at DESC LIMIT 1) AS avatar_url,
                               CASE
                                   WHEN EXISTS (
                                       SELECT 1 FROM user_follower outgoing
                                       INNER JOIN user_follower incoming
                                           ON incoming.follower_id = outgoing.following_id
                                          AND incoming.following_id = :viewerId
                                       WHERE outgoing.follower_id = :viewerId
                                         AND outgoing.following_id = candidate.user_id
                                   ) THEN 0
                                   WHEN EXISTS (
                                       SELECT 1 FROM user_follower relation
                                       WHERE (relation.follower_id = :viewerId AND relation.following_id = candidate.user_id)
                                          OR (relation.follower_id = candidate.user_id AND relation.following_id = :viewerId)
                                   ) THEN 1
                                   ELSE 2
                               END AS relationship_priority
                        FROM user_details candidate
                        WHERE candidate.user_id <> :viewerId
                          AND ((:emptyQuery = 1 AND EXISTS (
                                  SELECT 1 FROM user_follower outgoing
                                  INNER JOIN user_follower incoming
                                      ON incoming.follower_id = outgoing.following_id
                                     AND incoming.following_id = :viewerId
                                  WHERE outgoing.follower_id = :viewerId
                                    AND outgoing.following_id = candidate.user_id
                              ))
                              OR (:emptyQuery = 0 AND candidate.username LIKE :queryPattern))
                        ORDER BY relationship_priority ASC,
                                 candidate.username ASC,
                                 candidate.user_id ASC
                        LIMIT :limit
                        """)
                .bind("viewerId", viewerId)
                .bind("emptyQuery", emptyQuery ? 1 : 0)
                .bind("queryPattern", emptyQuery ? "" : queryPattern)
                .bind("limit", limit)
                .map((row, metadata) -> new ChatUserSuggestionResponse(
                        row.get("user_id", String.class),
                        row.get("username", String.class),
                        row.get("full_name", String.class),
                        row.get("avatar_url", String.class)))
                .all();
    }
}
