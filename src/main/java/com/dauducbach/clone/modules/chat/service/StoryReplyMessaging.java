package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import reactor.core.publisher.Mono;

import java.time.Instant;

public interface StoryReplyMessaging {

    Mono<ChatMessageResponse> send(StoryReplyCommand command);

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
}
