package com.dauducbach.clone.modules.user.dto.response;

public record StoryReplyResponse(
        String conversationId,
        String messageId,
        long messageSeq) {
}
