package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.MessageReactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import static com.dauducbach.clone.modules.chat.publicapi.ChatEventType.*;

@Service
@RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
public class ChatRealtimeLocalDispatcher {
    private static final Logger log = LoggerFactory.getLogger(ChatRealtimeLocalDispatcher.class);

    private final ObjectMapper objectMapper;
    private final ChatSessionRegistry sessionRegistry;
    private final MessageReactionRepository reactions;
    private final ConversationMemberRepository members;

    public ChatRealtimeLocalDispatcher(ObjectMapper objectMapper, ChatSessionRegistry sessionRegistry) {
        this(objectMapper, sessionRegistry, null, null);
    }

    public ChatRealtimeLocalDispatcher(ObjectMapper objectMapper, ChatSessionRegistry sessionRegistry,
            MessageReactionRepository reactions) {
        this(objectMapper, sessionRegistry, reactions, null);
    }

    public Mono<Void> dispatch(String payload) {
        return Mono.fromCallable(() -> objectMapper.readValue(payload, ChatEvent.class))
                .flatMap(event -> {
                    if (event.message() != null && (
                            event.type() == MESSAGE_CREATED
                            || event.type() == MESSAGE_DELETED)) {
                        return dispatchMessage(event, payload);
                    }
                    return recipients(event)
                            .filter(recipientId -> recipientId != null && !recipientId.isBlank())
                            .distinct()
                            .doOnNext(recipientId -> sessionRegistry.sendToUser(recipientId, payload))
                            .then();
                })
                .doOnError(error -> log.error(
                        "|ChatRealtimeLocalDispatcher|dispatch|failed|error={}", error.getMessage()));
    }

    private Flux<String> recipients(ChatEvent event) {
        if (event.type() == MESSAGE_REACTION_CHANGED) {
            if (reactions == null || event.reactionState() == null) return Flux.empty();
            return reactions.eligibleRecipients(event.conversationId(), event.reactionState().messageSeq())
                    .filter(event.recipientIds()::contains);
        }
        if (event.type() == CURSOR_UPDATED || event.type() == PINS_CHANGED || event.type() == MESSAGE_DELETED) {
            if (reactions == null) return event.type() == CURSOR_UPDATED
                    ? Flux.fromIterable(event.recipientIds()) : Flux.empty();
            return reactions.eligibleRecipients(event.conversationId(), Long.MAX_VALUE)
                    .filter(event.recipientIds()::contains);
        }
        return Flux.fromIterable(event.recipientIds());
    }

    private Mono<Void> dispatchMessage(ChatEvent event, String payload) {
        if (!java.util.Objects.equals(event.conversationId(), event.message().conversationId())) return Mono.empty();
        if (members == null) {
            if (reactions == null) return Mono.empty();
            return reactions.eligibleRecipients(event.conversationId(), event.message().messageSeq())
                    .filter(event.recipientIds()::contains).distinct()
                    .doOnNext(userId -> sessionRegistry.sendToUser(userId, payload)).then();
        }
        // Check history at the final delivery boundary for ordinary and forwarded messages alike.
        return members.findVisibleActiveMembers(event.conversationId(), event.message().messageSeq())
                .filter(member -> event.recipientIds().contains(member.getUserId()))
                .concatMap(member -> {
                    long visibleFrom = ChatVisibility.visibleFromSequence(member.getJoinedSeq(), member.getLastDeletedMessageSeq());
                    if (event.message().reply() == null || event.message().reply().messageSeq() >= visibleFrom) {
                        sessionRegistry.sendToUser(member.getUserId(), payload);
                        return Mono.empty();
                    }
                    var visibleEvent = new ChatEvent(event.type(), event.eventId(), event.conversationId(),
                            event.actorId(), event.entityId(), event.targetUserId(), event.occurredAt(), event.recipientIds(),
                            event.message().withReply(new com.dauducbach.clone.modules.chat.dto.response.ReplyMessageResponse(
                                    event.message().reply().messageSeq(), null, null, null, null, null, true)),
                            event.deliveredSeq(), event.readSeq(), event.reactionState(), event.pinVersion());
                    return Mono.fromCallable(() -> objectMapper.writeValueAsString(visibleEvent))
                            .doOnNext(filteredPayload -> sessionRegistry.sendToUser(member.getUserId(), filteredPayload)).then();
                }).then();
    }
}
