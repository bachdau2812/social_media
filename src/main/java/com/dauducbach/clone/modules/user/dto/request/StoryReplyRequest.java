package com.dauducbach.clone.modules.user.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.PositiveOrZero;

public record StoryReplyRequest(
        @NotBlank String content,
        @NotBlank String clientMessageId,
        @NotNull @PositiveOrZero Long previewAtMs) {
}
