package com.dauducbach.clone.modules.chat.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

public record CreateGroupConversationRequest(
        @NotBlank @Size(max = 255) String title,
        @NotEmpty List<@NotBlank String> initialUserIds) {
}
