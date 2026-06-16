package com.dauducbach.clone.modules.post.dto.request;

import jakarta.validation.constraints.NotBlank;

public record LikeRequest(
        @NotBlank(message = "targetId is required")
        String targetId,

        @NotBlank(message = "targetType is required")
        String targetType
) {
}
