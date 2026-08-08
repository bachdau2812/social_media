package com.dauducbach.clone.modules.media.dto.music.request;

import jakarta.validation.constraints.NotBlank;

import java.time.LocalDate;

public record MusicCreateRequest(
        @NotBlank(message = "displayName is required")
        String displayName,

        String descriptions,
        String displayImages,
        String singleName,

        @NotBlank(message = "songUrl is required")
        String songUrl,

        Long duration,
        String category,
        LocalDate releaseDate
) {
}

