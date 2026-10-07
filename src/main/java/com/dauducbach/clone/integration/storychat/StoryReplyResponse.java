package com.dauducbach.clone.integration.storychat;

public record StoryReplyResponse(
        String conversationId,
        String messageId,
        long messageSeq) {
}
