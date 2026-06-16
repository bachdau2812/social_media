package com.dauducbach.clone.modules.notification.dto.response;

import java.time.Instant;

public record PushTokenRegisterResponse(
        String id,
        String userId,
        String deviceId,
        Instant createdAt
) {
}
