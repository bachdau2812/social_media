package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.ForwardMessageRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

@Service
@RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
public class ChatForwardService {
    private final ChatMessageAccess access;
    private final ChatMessageActionsRepository actions;
    private final ChatMessageRepository messages;
    private final ChatMessageWriter writer;
    private final ConversationMemberRepository members;
    private final ChatResponseMapper mapper;
    private final TransactionalOperator tx;
    private final ChatMessageQueryService hydration;

    public ChatForwardService(ChatMessageAccess access, ChatMessageActionsRepository actions,
            ChatMessageRepository messages, ChatMessageWriter writer,
            ConversationMemberRepository members, ChatResponseMapper mapper, TransactionalOperator tx) {
        this(access, actions, messages, writer, members, mapper, tx, null);
    }

    public Mono<ChatMessageResponse> forward(String actor, String destination, ForwardMessageRequest request) {
        if (request == null || request.sourceConversationId() == null || request.sourceConversationId().isBlank()
                || request.sourceMessageId() == null || request.sourceMessageId().isBlank()) {
            return Mono.error(ChatMessageAccess.invalid("Forward source is required"));
        }
        try {
            if (!UUID.fromString(request.clientMessageId()).toString().equals(request.clientMessageId())) {
                throw new IllegalArgumentException();
            }
        } catch (RuntimeException error) {
            return Mono.error(ChatMessageAccess.invalid("clientMessageId must be a UUID"));
        }
        if (destination == null || destination.isBlank()) {
            return Mono.error(ChatMessageAccess.invalid("Destination is required"));
        }
        List<String> ids = Stream.of(destination, request.sourceConversationId()).distinct().sorted().toList();
        // Serial acquisition is intentional: reciprocal forwards and membership mutations share this lock order.
        return tx.transactional(Mono.defer(() -> Flux.fromIterable(ids)
                .concatMap(access::lockConversation).collectMap(Conversation::getId)
                .flatMap(locked -> forwardLocked(actor, destination, request, locked))));
    }

    private Mono<ChatMessageResponse> forwardLocked(String actor, String destination,
            ForwardMessageRequest request, Map<String, Conversation> locked) {
        return access.member(locked.get(request.sourceConversationId()), actor, false)
                .flatMap(source -> access.member(locked.get(destination), actor, true)
                        .flatMap(target -> {
                            if (target.conversation().getConversationType() != ConversationType.DIRECT) {
                                return Mono.error(ChatMessageAccess.invalid("Forward destination must be DIRECT"));
                            }
                            return members.findActiveUserIds(destination).distinct().collectList()
                                    .flatMap(users -> forwardToMembers(actor, request, source, target, users));
                        }));
    }

    private Mono<ChatMessageResponse> forwardToMembers(String actor, ForwardMessageRequest request,
            ChatMessageAccess.Context source, ChatMessageAccess.Context target, List<String> users) {
        if (users.size() != 2 || !users.contains(actor)) {
            return Mono.error(ChatMessageAccess.invalid("Destination requires an active peer"));
        }
        return messages.findBySenderIdAndClientMessageId(actor, request.clientMessageId())
                .flatMap(existing -> existingResponse(existing, target, request))
                .switchIfEmpty(Mono.defer(() -> access.message(source, request.sourceMessageId())
                        .flatMap(original -> createForward(actor, request, target, original, users))));
    }

    private Mono<ChatMessageResponse> existingResponse(ChatMessage existing,
            ChatMessageAccess.Context target, ForwardMessageRequest request) {
        if (!target.conversation().getId().equals(existing.getConversationId()) || !existing.isForwarded()) {
            return Mono.error(ChatMessageAccess.invalid("clientMessageId belongs to another message"));
        }
        if (!access.visible(target, existing)) {
            return Mono.error(ChatMessageAccess.forbidden());
        }
        // Once accepted, a copy is independent of source recall. Provenance still must match on every retry.
        return actions.matchesForward(existing.getId(), request.sourceConversationId(), request.sourceMessageId())
                .flatMap(matches -> {
                    if (!matches) {
                        return Mono.error(ChatMessageAccess.invalid("clientMessageId source mismatch"));
                    }
                    ChatMessageResponse response = mapper.toChatMessageResponse(existing);
                    if (hydration == null) {
                        return Mono.just(response);
                    }
                    return hydration.hydrateMessages(target.member().getUserId(),
                                    target.conversation().getId(), List.of(response))
                            .map(List::getFirst);
                });
    }

    private Mono<ChatMessageResponse> createForward(String actor, ForwardMessageRequest request,
            ChatMessageAccess.Context target, ChatMessage original, List<String> users) {
        MessageType type = original.getMessageType();
        if (original.getDeletedAt() != null
                || !(type == MessageType.TEXT || type == MessageType.IMAGE || type == MessageType.AUDIO)) {
            return Mono.error(ChatMessageAccess.forbidden());
        }
        ChatMessage copy = ChatMessage.builder()
                .id(UUID.randomUUID().toString()).conversationId(target.conversation().getId())
                .senderId(actor).clientMessageId(request.clientMessageId())
                .messageSeq(target.conversation().getLastMessageSeq() + 1)
                .messageType(type).content(original.getContent())
                .metadata(type == MessageType.TEXT ? null : original.getMetadata())
                .forwarded(true).createdAt(Instant.now()).build();
        Mono<Void> attachments = type == MessageType.TEXT ? Mono.empty()
                : Mono.defer(() -> actions.referenceMedia(copy.getId(), original.getId()))
                        .flatMap(count -> count > 0 ? Mono.empty()
                                : Mono.error(ChatMessageAccess.invalid("Source attachment is unavailable")));
        return writer.write(copy, target.conversation(), attachments,
                        users.stream().filter(user -> !actor.equals(user)).distinct().toList())
                .flatMap(saved -> actions.recordForward(saved.getId(), request.sourceConversationId(), original.getId())
                        .thenReturn(mapper.toChatMessageResponse(saved)));
    }
}
