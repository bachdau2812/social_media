package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationQuery;
import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationConversation;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ChatNotificationQueryService implements ChatNotificationQuery {
    private final ConversationRepository conversationRepository;
    private final ConversationMemberRepository conversationMemberRepository;

    @Override
    public Mono<Boolean> canReceiveMessageNotification(String conversationId, String userId, Instant now) {
        Instant evaluationTime = now == null ? Instant.now() : now;
        return conversationMemberRepository.findActive(conversationId, userId)
                .map(member -> member.getMutedUntil() == null || member.getMutedUntil().isBefore(evaluationTime))
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Boolean> isActiveMember(String conversationId, String userId) {
        return conversationMemberRepository.findActive(conversationId, userId)
                .map(ignored -> true)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<String> getConversationTitle(String conversationId, String fallback) {
        String safeFallback = fallback == null || fallback.isBlank() ? "Nhóm chat" : fallback;
        return conversationRepository.findById(conversationId)
                .map(conversation -> firstNonBlank(conversation.getTitle(), safeFallback))
                .defaultIfEmpty(safeFallback);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "";
    }

    @Override
    public Mono<ChatNotificationConversation> findConversation(String conversationId) {
        return conversationRepository.findById(conversationId)
                .map(conversation -> new ChatNotificationConversation(
                        conversation.getConversationType() == ConversationType.GROUP,
                        firstNonBlank(conversation.getTitle(), "Nhóm chat")));
    }
}
