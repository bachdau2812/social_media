package com.dauducbach.clone.modules.notification.service;

import java.util.Map;

public record NotificationPushPayload(
        String token,
        String title,
        String body,
        Map<String, String> data,
        String tag
) {
}
