package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.notification.dto.response.NotificationItemResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class NotificationQueryService {
    private final DatabaseClient databaseClient;
    private final NotificationMetadataCodec metadataCodec;

    public Mono<PageResponse<NotificationItemResponse>> getNotifications(String userId, String filter, int page, int size) {
        int safePage = Math.max(page, 0);
        int safeSize = size <= 0 ? 20 : Math.min(size, 100);
        int offset = safePage * safeSize;
        String where = buildWhere(filter);
        String listSql = """
                SELECT un.id,
                       un.user_id,
                       ne.actor_id,
                       actor.username AS actor_username,
                       COALESCE(NULLIF(actor.full_name, ''), actor.username) AS actor_display_name,
                       (SELECT COALESCE(av.secure_url, av.url)
                        FROM media av
                        WHERE av.owner_id = ne.actor_id AND av.owner_type = 'AVATAR'
                        ORDER BY av.created_at DESC
                        LIMIT 1) AS actor_avatar_url,
                       ne.action_type,
                       ne.entity_id,
                       ne.entity_type,
                       ne.content,
                       ne.metadata,
                       ne.deep_link,
                       COALESCE(
                           (SELECT COALESCE(pm.secure_url, pm.url)
                            FROM media pm
                            WHERE pm.owner_id = ne.entity_id AND pm.owner_type = 'POST'
                            ORDER BY pm.created_at ASC
                            LIMIT 1),
                           (SELECT us.media_url
                            FROM user_stories us
                            WHERE us.id = ne.entity_id
                            ORDER BY us.created_at DESC
                            LIMIT 1)
                       ) AS content_thumbnail_url,
                       CASE
                           WHEN ne.entity_id IS NULL THEN TRUE
                           WHEN UPPER(COALESCE(ne.entity_type, '')) = 'POST' THEN EXISTS (SELECT 1 FROM post_details pd WHERE pd.post_id = ne.entity_id)
                           WHEN UPPER(COALESCE(ne.entity_type, '')) = 'COMMENT' THEN EXISTS (SELECT 1 FROM comments c WHERE c.id = ne.entity_id)
                           WHEN UPPER(COALESCE(ne.entity_type, '')) = 'USER' THEN EXISTS (SELECT 1 FROM user_details target_user WHERE target_user.user_id = ne.entity_id)
                           WHEN UPPER(COALESCE(ne.entity_type, '')) = 'STORY' THEN EXISTS (SELECT 1 FROM user_stories target_story WHERE target_story.id = ne.entity_id)
                           ELSE TRUE
                       END AS entity_available,
                       un.notification_status,
                       un.read_at,
                       un.created_at
                FROM user_notifications un
                LEFT JOIN notification_events ne ON ne.id = un.event_id
                LEFT JOIN user_details actor ON actor.user_id = ne.actor_id
                """ + where + " ORDER BY un.created_at DESC, un.id DESC LIMIT :limit OFFSET :offset";
        String countSql = """
                SELECT COUNT(*)
                FROM user_notifications un
                LEFT JOIN notification_events ne ON ne.id = un.event_id
                """ + where;

        Mono<Long> total = databaseClient.sql(countSql)
                .bind("userId", userId)
                .map((row, metadata) -> ((Number) row.get(0)).longValue())
                .one()
                .defaultIfEmpty(0L);

        return total.flatMap(count -> databaseClient.sql(listSql)
                .bind("userId", userId)
                .bind("limit", safeSize)
                .bind("offset", offset)
                .map((row, metadata) -> new NotificationItemResponse(
                        row.get("id", String.class),
                        row.get("user_id", String.class),
                        row.get("actor_id", String.class),
                        row.get("actor_username", String.class),
                        row.get("actor_display_name", String.class),
                        row.get("actor_avatar_url", String.class),
                        string(row.get("action_type")),
                        row.get("entity_id", String.class),
                        row.get("entity_type", String.class),
                        row.get("content_thumbnail_url", String.class),
                        bool(row.get("entity_available")),
                        string(row.get("notification_status")),
                        instant(row.get("read_at")),
                        instant(row.get("created_at")),
                        row.get("content", String.class),
                        metadataCodec.decode(row.get("metadata", String.class)),
                        row.get("deep_link", String.class)))
                .all()
                .collectList()
                .map(items -> PageResponse.of(items, safePage, count, safeSize)));
    }

    public Mono<Long> unreadCount(String userId) {
        return databaseClient.sql("SELECT COUNT(*) FROM user_notifications WHERE user_id = :userId AND notification_status <> 'READ'")
                .bind("userId", userId)
                .map((row, metadata) -> ((Number) row.get(0)).longValue())
                .one()
                .defaultIfEmpty(0L);
    }

    public Mono<String> markRead(String notificationId, String userId) {
        return databaseClient.sql("SELECT user_id FROM user_notifications WHERE id = :id")
                .bind("id", notificationId)
                .map((row, metadata) -> row.get("user_id", String.class))
                .one()
                .switchIfEmpty(Mono.error(new com.dauducbach.clone.commons.exception.AppException(
                        com.dauducbach.clone.commons.exception.ErrorCode.USER_NOT_FOUND,
                        "Notification not found"
                )))
                .flatMap(ownerId -> {
                    if (!userId.equals(ownerId)) {
                        return Mono.error(new com.dauducbach.clone.commons.exception.AppException(
                                com.dauducbach.clone.commons.exception.ErrorCode.AUTHENTICATION_FAILED,
                                "Notification does not belong to the authenticated user"
                        ));
                    }
                    return databaseClient.sql("UPDATE user_notifications SET notification_status = 'READ', read_at = :readAt WHERE id = :id")
                            .bind("readAt", Instant.now())
                            .bind("id", notificationId)
                            .fetch()
                            .rowsUpdated()
                            .thenReturn("OK");
                });
    }

    public Mono<String> markAllRead(String userId) {
        return databaseClient.sql("UPDATE user_notifications SET notification_status = 'READ', read_at = :readAt WHERE user_id = :userId AND notification_status <> 'READ'")
                .bind("readAt", Instant.now())
                .bind("userId", userId)
                .fetch()
                .rowsUpdated()
                .thenReturn("OK");
    }

    private String buildWhere(String filter) {
        String base = " WHERE un.user_id = :userId ";
        String normalized = filter == null ? "ALL" : filter.trim().toUpperCase();
        return switch (normalized) {
            case "UNREAD" -> base + " AND un.notification_status <> 'READ' ";
            case "INTERACTIONS" -> base + " AND UPPER(COALESCE(ne.action_type, '')) IN ('LIKE', 'LIKES', 'LIKE_COMMENT', 'COMMENT', 'COMMENTS', 'REPLY_COMMENT', 'MENTION', 'TAG', 'UP_STORY', 'STORY_INTERACTION', 'FEATURED_STORY_INTERACTION', 'POST_SHARED') ";
            case "CONNECTIONS" -> base + " AND UPPER(COALESCE(ne.action_type, '')) IN ('FOLLOW', 'FOLLOW_EVENT', 'FOLLOW_BACK', 'ADD_FRIEND', 'ACCEPT_FRIEND', 'MUTUAL_FRIENDSHIP_CREATED') ";
            case "SYSTEM" -> base + " AND (ne.action_type IS NULL OR UPPER(ne.action_type) IN ('SYSTEM', 'REGISTRATION', 'LOGIN', 'LOGOUT', 'FORGOT_PASSWORD', 'RESET_PASSWORD', 'RESET_PASSWORD_AND_USERNAME', 'WELCOME_USER', 'SECURITY')) ";
            default -> base;
        };
    }

    private String string(Object value) {
        return value == null ? null : value.toString();
    }

    private boolean bool(Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof Number number) {
            return number.intValue() != 0;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private Instant instant(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof java.time.LocalDateTime localDateTime) {
            return localDateTime.atZone(java.time.ZoneId.systemDefault()).toInstant();
        }
        if (value instanceof java.time.OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof java.time.ZonedDateTime zonedDateTime) {
            return zonedDateTime.toInstant();
        }
        return Instant.parse(value.toString());
    }
}
