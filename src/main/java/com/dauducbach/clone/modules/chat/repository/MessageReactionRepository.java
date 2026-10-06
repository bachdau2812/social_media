package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.constant.ReactionType;
import com.dauducbach.clone.modules.chat.dto.response.*;
import io.r2dbc.spi.Row;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class MessageReactionRepository {
    private final DatabaseClient databaseClient;

    public static final String SET_SQL = """
            INSERT INTO message_reactions (message_id, user_id, reaction, reacted_at)
            VALUES (:messageId, :userId, :reaction, :reactedAt)
            ON DUPLICATE KEY UPDATE reaction = :reaction, reacted_at = :reactedAt
            """;

    // A single statement observes counts, viewer selection and revision from the same DB snapshot.
    public static final String SNAPSHOTS_SQL = """
            SELECT m.id, m.message_seq, m.reaction_version,
                   MAX(CASE WHEN r.user_id = :viewerId THEN r.reaction ELSE NULL END) AS my_reaction,
                   SUM(CASE WHEN r.reaction = 'HEART' THEN 1 ELSE 0 END) AS heart_count,
                   SUM(CASE WHEN r.reaction = 'LIKE' THEN 1 ELSE 0 END) AS like_count,
                   SUM(CASE WHEN r.reaction = 'HAHA' THEN 1 ELSE 0 END) AS haha_count,
                   SUM(CASE WHEN r.reaction = 'WOW' THEN 1 ELSE 0 END) AS wow_count,
                   SUM(CASE WHEN r.reaction = 'SAD' THEN 1 ELSE 0 END) AS sad_count,
                   SUM(CASE WHEN r.reaction = 'ANGRY' THEN 1 ELSE 0 END) AS angry_count
            FROM messages m
            JOIN conversation_members viewer ON viewer.conversation_id = m.conversation_id
                 AND viewer.user_id = :viewerId AND viewer.member_status = 'ACTIVE'
            LEFT JOIN message_reactions r ON r.message_id = m.id
            WHERE m.conversation_id = :conversationId AND m.id IN (:messageIds)
              AND m.message_seq >= :visibleFrom AND m.message_seq >= viewer.joined_seq
              AND (viewer.last_deleted_message_seq IS NULL OR m.message_seq > viewer.last_deleted_message_seq)
              AND m.deleted_at IS NULL AND m.message_type <> 'SYSTEM'
            GROUP BY m.id, m.message_seq, m.reaction_version
            ORDER BY m.message_seq
            """;

    public Mono<MessageRow> lockMessage(String conversationId, String messageId) {
        return databaseClient.sql("""
                SELECT id, message_seq, message_type, deleted_at, reaction_version FROM messages
                WHERE conversation_id = :conversationId AND id = :messageId FOR UPDATE
                """).bind("conversationId", conversationId).bind("messageId", messageId)
                .map((row, metadata) -> new MessageRow(row.get("id", String.class), number(row, "message_seq"),
                        MessageType.valueOf(row.get("message_type", String.class)),
                        instant(row.get("deleted_at")), number(row, "reaction_version"))).one();
    }

    public Mono<ReactionType> findMine(String messageId, String userId) {
        return databaseClient.sql("SELECT reaction FROM message_reactions WHERE message_id = :messageId AND user_id = :userId")
                .bind("messageId", messageId).bind("userId", userId)
                .map((row, metadata) -> ReactionType.valueOf(row.get("reaction", String.class))).one();
    }

    public Mono<Void> set(String messageId, String userId, ReactionType reaction, Instant reactedAt) {
        return databaseClient.sql(SET_SQL).bind("messageId", messageId).bind("userId", userId)
                .bind("reaction", reaction.name()).bind("reactedAt", reactedAt).fetch().rowsUpdated().then();
    }

    public Mono<Void> remove(String messageId, String userId) {
        return databaseClient.sql("DELETE FROM message_reactions WHERE message_id = :messageId AND user_id = :userId")
                .bind("messageId", messageId).bind("userId", userId).fetch().rowsUpdated().then();
    }

    public Mono<Void> incrementVersion(String messageId) {
        return databaseClient.sql("UPDATE messages SET reaction_version = reaction_version + 1 WHERE id = :messageId")
                .bind("messageId", messageId).fetch().rowsUpdated().then();
    }

    public Mono<List<ReactionSnapshot>> snapshots(String conversationId, List<String> messageIds,
                                                 String viewerId, long visibleFrom) {
        if (messageIds.isEmpty()) return Mono.just(List.of());
        return databaseClient.sql(SNAPSHOTS_SQL).bind("conversationId", conversationId)
                .bind("messageIds", messageIds).bind("viewerId", viewerId).bind("visibleFrom", visibleFrom)
                .map((row, metadata) -> snapshot(row)).all().collectList();
    }

    private ReactionSnapshot snapshot(Row row) {
        List<ReactionCount> counts = new ArrayList<>();
        for (ReactionType type : ReactionType.values()) {
            long count = number(row, type.name().toLowerCase(java.util.Locale.ROOT) + "_count");
            if (count > 0) counts.add(new ReactionCount(type, count));
        }
        String mine = row.get("my_reaction", String.class);
        ReactionType myReaction = mine == null ? null : ReactionType.valueOf(mine);
        return new ReactionSnapshot(row.get("id", String.class), number(row, "message_seq"),
                number(row, "reaction_version"), myReaction, myReaction == ReactionType.HEART,
                number(row, "heart_count"), counts);
    }

    public Flux<String> eligibleRecipients(String conversationId, long messageSeq) {
        return databaseClient.sql("""
                SELECT user_id FROM conversation_members WHERE conversation_id = :conversationId
                AND member_status = 'ACTIVE' AND joined_seq <= :messageSeq
                AND (last_deleted_message_seq IS NULL OR last_deleted_message_seq < :messageSeq)
                ORDER BY user_id
                """).bind("conversationId", conversationId).bind("messageSeq", messageSeq)
                .map((row, metadata) -> row.get("user_id", String.class)).all();
    }

    public Flux<MessageReactorResponse> list(String messageId, ReactionType filter, String cursor, int limit) {
        // Immutable user IDs form a stable keyset; changing a reaction does not shift its paging key.
        String sql = """
                SELECT r.user_id, COALESCE(NULLIF(u.full_name, ''), NULLIF(u.username, ''), r.user_id) AS display_name,
                       (SELECT COALESCE(NULLIF(a.secure_url, ''), a.url) FROM media a
                        WHERE a.owner_id = r.user_id AND a.owner_type = 'AVATAR'
                        ORDER BY a.created_at DESC, a.asset_id DESC LIMIT 1) AS avatar_url,
                       r.reaction, r.reacted_at
                FROM message_reactions r LEFT JOIN user_details u ON u.user_id = r.user_id
                WHERE r.message_id = :messageId AND r.user_id > :cursor
                """ + (filter == null ? "" : " AND r.reaction = :reaction") + " ORDER BY r.user_id LIMIT :limit";
        var spec = databaseClient.sql(sql).bind("messageId", messageId).bind("cursor", cursor).bind("limit", limit);
        if (filter != null) spec = spec.bind("reaction", filter.name());
        return spec.map((row, metadata) -> new MessageReactorResponse(row.get("user_id", String.class),
                row.get("display_name", String.class), row.get("avatar_url", String.class),
                ReactionType.valueOf(row.get("reaction", String.class)), instant(row.get("reacted_at")))).all();
    }

    private static long number(Row row, String name) { return ((Number) row.get(name)).longValue(); }

    static Instant instant(Object value) {
        if (value == null) return null;
        if (value instanceof Instant instant) return instant;
        if (value instanceof LocalDateTime local) return local.atZone(ZoneId.systemDefault()).toInstant();
        throw new IllegalStateException("Unsupported chat timestamp " + value.getClass());
    }

    public record MessageRow(String id, long messageSeq, MessageType messageType,
                             Instant deletedAt, long reactionVersion) {}
}
