package com.dauducbach.clone.modules.post.dto.story.response;

public record StoryReplyResponse(
        String conversationId,
        String messageId,
        long messageSeq) {
}
