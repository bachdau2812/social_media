package com.dauducbach.clone.modules.chat.dto.request;

import jakarta.validation.constraints.NotBlank;

public record CreateDirectConversationRequest(@NotBlank String targetUserId) {
}
