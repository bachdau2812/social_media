package com.dauducbach.clone.modules.chat.dto.request;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record UpdateMessageRequest(@NotBlank @Size(max = 10000) String content) {
}
