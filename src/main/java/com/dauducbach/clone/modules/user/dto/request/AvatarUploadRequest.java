package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;

public record AvatarUploadRequest(
        @NotBlank(message = "userId is required")
        String userId,

        @NotBlank(message = "avatarUrl is required")
        String avatarUrl
) {
}
