package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;

public record JamendoMusicImportRequest(
        @NotBlank(message = "url is required")
        String url,

        String category
) {
}
