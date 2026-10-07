package com.dauducbach.clone.modules.notification.publicapi;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.notification.dto.response.NotificationItemResponse;
import reactor.core.publisher.Mono;

/** Read-only inbox operations used by the notification HTTP adapter. */
public interface NotificationQuery {
    Mono<PageResponse<NotificationItemResponse>> getNotifications(
            String userId,
            String filter,
            int page,
            int size
    );

    Mono<Long> unreadCount(String userId);

    Mono<String> markRead(String notificationId, String userId);

    Mono<String> markAllRead(String userId);
}
