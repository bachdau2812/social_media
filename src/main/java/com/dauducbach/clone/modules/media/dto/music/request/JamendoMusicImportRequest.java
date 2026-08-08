package com.dauducbach.clone.modules.media.dto.music.request;

import jakarta.validation.constraints.NotBlank;

public record JamendoMusicImportRequest(
        @NotBlank(message = "url is required")
        String url,

        String category
) {
}
