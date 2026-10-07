package com.dauducbach.clone.modules.chat.publicapi;

import reactor.core.publisher.Mono;

import java.time.Instant;

/** Read-only chat data needed by notification policies. */
public interface ChatNotificationQuery {
    Mono<Boolean> canReceiveMessageNotification(String conversationId, String userId, Instant now);

    Mono<Boolean> isActiveMember(String conversationId, String userId);

    Mono<String> getConversationTitle(String conversationId, String fallback);
}
