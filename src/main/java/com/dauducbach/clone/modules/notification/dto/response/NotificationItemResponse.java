package com.dauducbach.clone.modules.notification.dto.response;

import java.time.Instant;
import java.util.Map;

public record NotificationItemResponse(
        String id,
        String userId,
        String actorId,
        String actorUsername,
        String actorDisplayName,
        String actorAvatarUrl,
        String actionType,
        String entityId,
        String entityType,
        String contentThumbnailUrl,
        boolean entityAvailable,
        String status,
        Instant readAt,
        Instant createdAt,
        String content,
        Map<String, String> metadata,
        String deepLink
) {
}
