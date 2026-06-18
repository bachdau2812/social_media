package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;

public record StoryCreateRequest(
        @NotBlank(message = "userId is required")
        String userId,

        @NotBlank(message = "mediaUrl is required")
        String mediaUrl,

        String musicUrl,

        Long musicStart,

        Long musicEnd
) {
}
