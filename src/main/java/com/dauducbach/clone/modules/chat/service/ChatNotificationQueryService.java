package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ChatNotificationQueryService {
    private final ConversationRepository conversationRepository;
    private final ConversationMemberRepository conversationMemberRepository;

    public Mono<Boolean> canReceiveMessageNotification(String conversationId, String userId, Instant now) {
        Instant evaluationTime = now == null ? Instant.now() : now;
        return conversationMemberRepository.findActive(conversationId, userId)
                .map(member -> member.getMutedUntil() == null || member.getMutedUntil().isBefore(evaluationTime))
                .defaultIfEmpty(false);
    }

    public Mono<Boolean> isActiveMember(String conversationId, String userId) {
        return conversationMemberRepository.findActive(conversationId, userId)
                .map(ignored -> true)
                .defaultIfEmpty(false);
    }

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
}