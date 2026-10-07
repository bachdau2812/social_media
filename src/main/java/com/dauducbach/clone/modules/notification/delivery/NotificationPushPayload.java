package com.dauducbach.clone.modules.notification.delivery;

import java.util.Map;

public record NotificationPushPayload(
        String token,
        String title,
        String body,
        Map<String, String> data,
        String tag
) {
}
