package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
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
    private final ChatResponseMapper responseMapper;

    public ChatOutboxPublisher(ChatOutboxRepository repository,ChatEventPublisher kafka,
            MessageReactionRepository reactions,ObjectMapper mapper) {
        this(repository,kafka,reactions,mapper,null,null);
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
        if(messages==null || event.type()!=com.dauducbach.clone.modules.chat.constant.ChatEventType.MESSAGE_CREATED
                || event.message()==null)return Mono.just(event);
        // A creation can wait in the queue while its accepted copy is recalled. Never replay stale private content.
        return messages.findById(event.message().id()).map(message -> {
            // Keep the original coherent creation snapshot while live. A newer revision without its counts is false state.
            if (message.getDeletedAt() == null) {
                return event;
            }
            return new ChatEvent(com.dauducbach.clone.modules.chat.constant.ChatEventType.MESSAGE_DELETED,
                    event.eventId(), event.conversationId(), event.actorId(), event.entityId(), event.targetUserId(),
                    event.occurredAt(), event.recipientIds(), responseMapper.toChatMessageResponse(message).neutralReactions(),
                    event.deliveredSeq(), event.readSeq(), event.reactionState(), event.pinVersion());
        });
    }
}
