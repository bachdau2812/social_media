package com.dauducbach.clone.modules.chat.publicapi;

import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface StoryReplySender {

    Mono<StoryReplyMessage> send(StoryReplyCommand command);

    record StoryReplyCommand(
            String senderId,
            String storyId,
            String ownerId,
            String content,
            String clientMessageId,
            String mediaType,
            long previewAtMs,
            Instant expiresAt) {
    }

    record StoryReplyMessage(String conversationId, String messageId, long messageSeq) { }
}
