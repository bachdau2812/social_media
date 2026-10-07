package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.MessageReactionRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
public class ChatOutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(ChatOutboxPublisher.class);
    private final ChatOutboxRepository repository;
    private final ChatEventPublisher kafka;
    private final MessageReactionRepository reactions;
    private final ObjectMapper mapper;
    private final com.dauducbach.clone.modules.chat.repository.ChatMessageRepository messages;
    private final com.dauducbach.clone.modules.chat.repository.ChatReadRepository reads;
    private final ChatResponseMapper responseMapper;

    public ChatOutboxPublisher(ChatOutboxRepository repository,ChatEventPublisher kafka,
            MessageReactionRepository reactions,ObjectMapper mapper) {
        this(repository,kafka,reactions,mapper,null,null,null);
    }

    public ChatOutboxPublisher(ChatOutboxRepository repository,ChatEventPublisher kafka,
            MessageReactionRepository reactions,ObjectMapper mapper,
            com.dauducbach.clone.modules.chat.repository.ChatMessageRepository messages,
            ChatResponseMapper responseMapper) {
        this(repository,kafka,reactions,mapper,messages,null,responseMapper);
    }

    @Scheduled(fixedDelayString = "${chat.outbox.poll-delay-ms:${chat.reactions.outbox.poll-delay-ms:1000}}")
    public Mono<Void> scheduledDrain() { return Mono.defer(this::drain); }

    public Mono<Void> drain() {
        return Mono.defer(() -> repository.candidates(Instant.now())
                .concatMap(id -> repository.lease(id, UUID.randomUUID().toString(), Instant.now())
                        .flatMap(this::publishLeased)).then());
    }

    private Mono<Void> publishLeased(ChatOutboxRepository.LeasedEvent lease) {
        return Mono.fromCallable(() -> mapper.readValue(lease.payload(), ChatEvent.class))
                .flatMap(this::refreshCreatedMessage)
                .flatMap(event -> reactions.eligibleRecipients(event.conversationId(), event.reactionState() != null ? event.reactionState().messageSeq() : event.message() != null ? event.message().messageSeq() : Long.MAX_VALUE)
                        .filter(event.recipientIds()::contains).distinct().collectList()
                        .flatMap(recipients -> recipients.isEmpty() ? Mono.empty() : kafka.publish(new ChatEvent(
                                event.type(), event.eventId(), event.conversationId(), event.actorId(), event.entityId(),
                                event.targetUserId(), event.occurredAt(), recipients, event.message(), event.deliveredSeq(), event.readSeq(), event.reactionState(), event.pinVersion()))))
                .timeout(Duration.ofSeconds(45))
                .then(Mono.defer(() -> repository.complete(lease)))
                .onErrorResume(error -> {
                    log.warn("Chat event publish failed; retaining event {} for retry: {}", lease.id(), error.getMessage());
                    long delaySeconds = Math.min(300, 1L << Math.min(8, lease.attempts()));
                    return repository.retry(lease, Instant.now().plusSeconds(delaySeconds));
                });
    }

    private Mono<ChatEvent> refreshCreatedMessage(ChatEvent event) {
        if(messages==null || event.type()!=com.dauducbach.clone.modules.chat.publicapi.ChatEventType.MESSAGE_CREATED
                || event.message()==null)return Mono.just(event);
        // The queued event contains only its accepted payload. Hydrate display/reply fields at delivery time.
        // If recalled while waiting, publish the tombstone so a stale creation cannot restore its body.
        return messages.findById(event.message().id())
                .flatMap(message -> {
                    if (reads == null) {
                        if (message.getDeletedAt() == null) return Mono.just(event);
                        return Mono.just(withMessage(event,
                                com.dauducbach.clone.modules.chat.publicapi.ChatEventType.MESSAGE_DELETED,
                                responseMapper.toChatMessageResponse(message).neutralReactions()));
                    }
                    return reads.findAfterSequence(event.conversationId(), 1L,
                                    Math.max(0L, event.message().messageSeq() - 1L), 1)
                            .next()
                            .map(responseMapper::toChatMessageResponse)
                            .map(response -> withMessage(event,
                                    message.getDeletedAt() == null
                                            ? com.dauducbach.clone.modules.chat.publicapi.ChatEventType.MESSAGE_CREATED
                                            : com.dauducbach.clone.modules.chat.publicapi.ChatEventType.MESSAGE_DELETED,
                                    response.neutralReactions()))
                            .defaultIfEmpty(message.getDeletedAt() == null ? event : withMessage(event,
                                    com.dauducbach.clone.modules.chat.publicapi.ChatEventType.MESSAGE_DELETED,
                                    responseMapper.toChatMessageResponse(message).neutralReactions()));
                })
                .defaultIfEmpty(event);
    }

    private ChatEvent withMessage(ChatEvent event,
            com.dauducbach.clone.modules.chat.publicapi.ChatEventType type,
            com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse message) {
        return new ChatEvent(type, event.eventId(), event.conversationId(), event.actorId(), event.entityId(),
                event.targetUserId(), event.occurredAt(), event.recipientIds(), message, event.deliveredSeq(),
                event.readSeq(), event.reactionState(), event.pinVersion());
    }
}
