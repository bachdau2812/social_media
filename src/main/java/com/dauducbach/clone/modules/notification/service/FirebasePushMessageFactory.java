package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.modules.notification.dto.NotificationForService;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class FirebasePushMessageFactory {

    public NotificationPushPayload create(
            String deviceToken,
            String notificationId,
            String dedupTag,
            String deepLink,
            NotificationForService notification
    ) {
        Map<String, String> data = new LinkedHashMap<>();
        data.put("notificationId", notificationId);
        data.put("actionType", notification.getActionType() == null ? "" : notification.getActionType().name());
        data.put("url", deepLink == null || deepLink.isBlank() ? "/" : deepLink);

        return new NotificationPushPayload(
                deviceToken,
                notification.getTitle() == null ? "" : notification.getTitle(),
                notification.getHtmlContent() == null ? "" : notification.getHtmlContent(),
                Map.copyOf(data),
                dedupTag);
    }
}
