package com.dauducbach.clone.modules.notification.dto.request;

import jakarta.validation.constraints.NotBlank;

public record PushTokenRegisterRequest(
        @NotBlank(message = "userId is required")
        String userId,

        String deviceId,

        @NotBlank(message = "deviceToken is required")
        String deviceToken
) {
}
