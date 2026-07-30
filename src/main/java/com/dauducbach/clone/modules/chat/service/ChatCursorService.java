package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.response.ChatCursorResponse;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ChatCursorService {
    private static final Logger log = LoggerFactory.getLogger(ChatCursorService.class);

    ChatAccessService accessService;
    ConversationMemberRepository memberRepository;
    ConversationRepository conversationRepository;
    ChatReadRepository chatReadRepository;
    ChatEventPublisher eventPublisher;

    public Mono<Void> markPendingDeliveredOnConnect(String actorId) {
        return chatReadRepository.findPendingDeliveries(actorId)
                .concatMap(cursor -> markDelivered(actorId, cursor.conversationId(), cursor.sequence())
                        .onErrorResume(error -> {
                            log.warn("|ChatCursorService|markPendingDeliveredOnConnect|skipped|actorId={}|conversationId={}|error={}",
                                    actorId, cursor.conversationId(), error.getMessage());
                            return Mono.empty();
                        }))
                .then();
    }
    public Mono<ChatCursorResponse> markDelivered(String actorId, String conversationId, long sequence) {
        validate(sequence);
        return validateSequence(conversationId, sequence)
                .then(accessService.requireActiveMember(conversationId, actorId))
                .flatMap(member -> memberRepository.advanceDeliveredSequence(conversationId, actorId, sequence)
                        .thenReturn(new ChatCursorResponse(
                                conversationId,
                                Math.max(member.getLastDeliveredSeq(), sequence),
                                member.getLastReadSeq())))
                .flatMap(response -> publishCursor(actorId, response).thenReturn(response))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_CURSOR_UPDATE_FAILED, "Update delivered cursor failed", error));
    }

    public Mono<ChatCursorResponse> markRead(String actorId, String conversationId, long sequence) {
        validate(sequence);
        return validateSequence(conversationId, sequence)
                .then(accessService.requireActiveMember(conversationId, actorId))
                .flatMap(member -> memberRepository.advanceDeliveredAndReadSequence(conversationId, actorId, sequence)
                        .thenReturn(new ChatCursorResponse(
                                conversationId,
                                Math.max(member.getLastDeliveredSeq(), sequence),
                                Math.max(member.getLastReadSeq(), sequence))))
                .flatMap(response -> publishCursor(actorId, response).thenReturn(response))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_CURSOR_UPDATE_FAILED, "Update read cursor failed", error));
    }

    private void validate(long sequence) {
        if (sequence <= 0) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_SEQUENCE_INVALID, "sequence must be positive");
        }
    }

    private Mono<Void> validateSequence(String conversationId, long sequence) {
        return conversationRepository.findById(conversationId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CONVERSATION_NOT_FOUND,
                        "Conversation not found")))
                .flatMap(conversation -> sequence <= conversation.getLastMessageSeq()
                        ? Mono.empty()
                        : Mono.error(new AppException(
                                ErrorCode.CHAT_MESSAGE_SEQUENCE_INVALID,
                                "sequence exceeds the latest conversation message")));
    }

    private Mono<Void> publishCursor(String actorId, ChatCursorResponse response) {
        return memberRepository.findActiveUserIds(response.conversationId())
                .filter(userId -> !actorId.equals(userId))
                .collectList()
                .flatMap(recipients -> eventPublisher.publish(
                        ChatEvent.cursorUpdated(response.conversationId(), actorId, recipients, response)))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100)))
                .onErrorResume(error -> {
                    log.error("|ChatCursorService|publishCursor|failed|conversationId={}|actorId={}|error={}",
                            response.conversationId(), actorId, error.getMessage());
                    return Mono.empty();
                });
    }
}
