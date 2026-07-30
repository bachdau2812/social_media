package com.dauducbach.clone.modules.chat.dto.request;

import jakarta.validation.constraints.Positive;

public record UpdateChatCursorRequest(@Positive long sequence) {
}
