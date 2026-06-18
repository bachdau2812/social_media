package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;

public record MusicSelectRequest(
        @NotBlank(message = "userId is required")
        String userId,

        @NotBlank(message = "musicDisplayName is required")
        String musicDisplayName,

        @NotBlank(message = "musicSlugName is required")
        String musicSlugName
) {
}
