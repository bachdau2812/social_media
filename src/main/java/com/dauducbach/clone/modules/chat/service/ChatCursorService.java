package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.response.ChatCursorResponse;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ChatCursorService {
    ChatAccessService accessService;
    ConversationMemberRepository memberRepository;
    ConversationRepository conversationRepository;
    ChatOutboxRepository outbox;
    TransactionalOperator transactionalOperator;

    public Mono<ChatCursorResponse> markDelivered(String actorId, String conversationId, long sequence) {
        validate(sequence);
        return transactionalOperator.transactional(validateSequence(conversationId, sequence)
                .then(accessService.requireActiveMember(conversationId, actorId))
                .flatMap(member -> memberRepository.advanceDeliveredSequence(conversationId, actorId, sequence)
                        .thenReturn(new ChatCursorResponse(
                                conversationId,
                                Math.max(member.getLastDeliveredSeq(), sequence),
                                member.getLastReadSeq())))
                .flatMap(response -> appendCursorEvent(actorId, response).thenReturn(response)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_CURSOR_UPDATE_FAILED, "Update delivered cursor failed", error));
    }

    public Mono<ChatCursorResponse> markRead(String actorId, String conversationId, long sequence) {
        validate(sequence);
        return transactionalOperator.transactional(validateSequence(conversationId, sequence)
                .then(accessService.requireActiveMember(conversationId, actorId))
                .flatMap(member -> memberRepository.advanceDeliveredAndReadSequence(conversationId, actorId, sequence)
                        .thenReturn(new ChatCursorResponse(
                                conversationId,
                                Math.max(member.getLastDeliveredSeq(), sequence),
                                Math.max(member.getLastReadSeq(), sequence))))
                .flatMap(response -> appendCursorEvent(actorId, response).thenReturn(response)))
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

    private Mono<Void> appendCursorEvent(String actorId, ChatCursorResponse response) {
        return memberRepository.findActiveUserIds(response.conversationId())
                .filter(userId -> !actorId.equals(userId))
                .collectList()
                .flatMap(recipients -> outbox.append(
                        ChatEvent.cursorUpdated(response.conversationId(), actorId, recipients, response)));
    }
}
