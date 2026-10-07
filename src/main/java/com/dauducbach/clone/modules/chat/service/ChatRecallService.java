package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.ChatMessageActionsRepository;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.MessageReactionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class ChatRecallService {
    private final ChatMessageAccess access;
    private final ChatMessageActionsRepository actions;
    private final ChatOutboxRepository outbox;
    private final MessageReactionRepository recipients;
    private final ChatResponseMapper mapper;
    private final TransactionalOperator tx;

    public Mono<ChatMessageResponse> recall(String actorId, String conversationId, String messageId) {
        return tx.transactional(Mono.defer(() -> access.context(actorId, conversationId, true)
                .flatMap(context -> access.message(context, messageId))
                .flatMap(message -> recallLocked(actorId, conversationId, message))));
    }

    private Mono<ChatMessageResponse> recallLocked(String actorId, String conversationId, ChatMessage message) {
        if (!actorId.equals(message.getSenderId()) || message.getMessageType() == MessageType.SYSTEM) {
            return Mono.error(ChatMessageAccess.forbidden());
        }
        if (message.getDeletedAt() != null) {
            return Mono.just(mapper.toChatMessageResponse(message));
        }
        Instant recalledAt = Instant.now();
        return actions.recall(message.getId(), recalledAt)
                .then(actions.removeReactions(message.getId()))
                .then(actions.removePin(conversationId, message.getId()))
                .flatMap(removedPins -> {
                    message.setDeletedAt(recalledAt);
                    message.setReactionVersion(message.getReactionVersion() + 1);
                    ChatMessageResponse response = mapper.toChatMessageResponse(message);
                    return recipients.eligibleRecipients(conversationId, message.getMessageSeq())
                            .distinct().collectList()
                            .flatMap(users -> outbox.append(ChatEvent.messageDeleted(response, actorId, users))
                                    .then(removedPins > 0 ? publishPinRemoval(actorId, conversationId) : Mono.empty())
                                    .thenReturn(response));
                });
    }

    private Mono<Void> publishPinRemoval(String actorId, String conversationId) {
        return actions.incrementPinVersion(conversationId)
                .then(actions.pinVersion(conversationId))
                .flatMap(version -> recipients.eligibleRecipients(conversationId, Long.MAX_VALUE)
                        .distinct().collectList()
                        .flatMap(users -> outbox.append(ChatEvent.pinsChanged(conversationId, actorId, version, users))));
    }
}
