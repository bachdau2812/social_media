package com.dauducbach.clone.modules.chat.dto.request;

public record ChatSocketClientFrame(
        String type,
        String conversationId,
        Long sequence
) {
}
