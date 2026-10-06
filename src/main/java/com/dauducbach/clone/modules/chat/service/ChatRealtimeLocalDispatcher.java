package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
public class ChatRealtimeLocalDispatcher {
    private static final Logger log = LoggerFactory.getLogger(ChatRealtimeLocalDispatcher.class);

    private final ObjectMapper objectMapper;
    private final ChatSessionRegistry sessionRegistry;
    private final com.dauducbach.clone.modules.chat.repository.MessageReactionRepository reactions;

    public ChatRealtimeLocalDispatcher(ObjectMapper objectMapper, ChatSessionRegistry sessionRegistry) {
        this(objectMapper, sessionRegistry, null);
    }

    public Mono<Void> dispatch(String payload) {
        return Mono.fromCallable(() -> objectMapper.readValue(payload, ChatEvent.class))
                .flatMapMany(event -> {
                    if (event.type() == com.dauducbach.clone.modules.chat.constant.ChatEventType.MESSAGE_REACTION_CHANGED) {
                        if (reactions == null || event.reactionState() == null) return reactor.core.publisher.Flux.empty();
                        return reactions.eligibleRecipients(event.conversationId(), event.reactionState().messageSeq())
                                .filter(event.recipientIds()::contains);
                    }
                    if (event.type() == com.dauducbach.clone.modules.chat.constant.ChatEventType.MESSAGE_DELETED
                            || event.type() == com.dauducbach.clone.modules.chat.constant.ChatEventType.PINS_CHANGED
                            || event.type() == com.dauducbach.clone.modules.chat.constant.ChatEventType.MESSAGE_CREATED && event.message()!=null && event.message().forwarded()) {
                        if(reactions==null)return reactor.core.publisher.Flux.empty();
                        return reactions.eligibleRecipients(event.conversationId(),event.message()==null?Long.MAX_VALUE:event.message().messageSeq())
                                .filter(event.recipientIds()::contains);
                    }
                    return reactor.core.publisher.Flux.fromIterable(event.recipientIds());
                })
                .filter(recipientId -> recipientId != null && !recipientId.isBlank())
                .distinct()
                .doOnNext(recipientId -> sessionRegistry.sendToUser(recipientId, payload))
                .doOnError(error -> log.error(
                "|ChatRealtimeLocalDispatcher|dispatch|failed|error={}",
                error.getMessage())).then();
    }
}
