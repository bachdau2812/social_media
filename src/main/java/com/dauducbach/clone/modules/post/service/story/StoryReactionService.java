package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.story.event.StoryLikeEventPayload;
import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repositoty.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class StoryReactionService {
    private static final String LIKE_REACTION = "LIKE";
    private static final String LIKE_EVENT_TOPIC = "like_event";

    private final UserStoriesRepository storiesRepository;
    private final StoryViewRepository viewRepository;
    private final R2dbcEntityTemplate entityTemplate;
    private final KafkaSender<String, String> kafkaSender;

    public Mono<Boolean> like(String storyId, String actorId) {
        String normalizedStoryId = requireText(storyId, "storyId");
        String normalizedActorId = requireText(actorId, "actorId");
        String candidateInteractionId = UUID.randomUUID().toString();
        log.info("|StoryReactionService|like|requested|storyId={}|actorId={}", normalizedStoryId, normalizedActorId);

        return activeStoryForReaction(normalizedStoryId, normalizedActorId)
                .flatMap(story -> persistLike(story.getId(), normalizedActorId, candidateInteractionId)
                        .doOnNext(result -> log.info(
                                "|StoryReactionService|like|likePersisted|storyId={}|actorId={}|interactionId={}|changed={}",
                                story.getId(), normalizedActorId,
                                result.view().getReactionInteractionId(), result.changed()))
                        .flatMap(result -> publishLikeEvent(new StoryLikeEventPayload(
                                        normalizedActorId,
                                        story.getId(),
                                        "STORY",
                                        story.getUserId(),
                                        result.view().getReactionInteractionId(),
                                        Instant.now()))
                                .thenReturn(result)))
                .doOnNext(result -> log.info(
                        "|StoryReactionService|like|completed|storyId={}|actorId={}|interactionId={}|changed={}",
                        normalizedStoryId,
                        normalizedActorId,
                        result.view().getReactionInteractionId(),
                        result.changed()))
                .map(LikePersistence::changed)
                .doOnError(error -> log.error(
                        "|StoryReactionService|like|failed|storyId={}|actorId={}|errorType={}",
                        normalizedStoryId, normalizedActorId, error.getClass().getSimpleName()))
                .doFinally(signal -> {
                    if (signal == SignalType.CANCEL) {
                        log.warn("|StoryReactionService|like|cancelled|storyId={}|actorId={}",
                                normalizedStoryId, normalizedActorId);
                    }
                });
    }

    public Mono<Boolean> unlike(String storyId, String actorId) {
        String normalizedStoryId = requireText(storyId, "storyId");
        String normalizedActorId = requireText(actorId, "actorId");
        return activeStoryForReaction(normalizedStoryId, normalizedActorId)
                .flatMap(story -> viewRepository.clearLike(story.getId(), normalizedActorId))
                .map(updated -> updated != null && updated > 0);
    }

    private Mono<Void> publishLikeEvent(StoryLikeEventPayload payload) {
        JsonObject event = new JsonObject();
        event.addProperty("actorId", payload.actorId());
        event.addProperty("targetId", payload.targetId());
        event.addProperty("targetType", payload.targetType());
        event.addProperty("targetOwnerId", payload.targetOwnerId());
        event.addProperty("interactionId", payload.interactionId());
        event.addProperty("timestamp", payload.timestamp().toString());
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(LIKE_EVENT_TOPIC, payload.targetId(), event.toString()),
                payload.interactionId());

        log.info(
                "|StoryReactionService|publishLikeEvent|sending|storyId={}|actorId={}|ownerId={}|interactionId={}",
                payload.targetId(), payload.actorId(), payload.targetOwnerId(), payload.interactionId());
        return kafkaSender.send(Mono.just(record))
                .single()
                .flatMap(result -> requireBrokerAcknowledgement(payload, result))
                .doOnError(error -> log.error(
                        "|StoryReactionService|publishLikeEvent|failed|storyId={}|actorId={}|ownerId={}|interactionId={}|errorType={}",
                        payload.targetId(), payload.actorId(), payload.targetOwnerId(), payload.interactionId(),
                        error.getClass().getSimpleName()));
    }

    private Mono<Void> requireBrokerAcknowledgement(
            StoryLikeEventPayload payload,
            SenderResult<String> result
    ) {
        if (result.exception() != null) {
            return Mono.error(result.exception());
        }
        var metadata = result.recordMetadata();
        if (metadata == null) {
            return Mono.error(new IllegalStateException("Kafka acknowledgement metadata is missing"));
        }
        log.info(
                "|StoryReactionService|publishLikeEvent|brokerAcknowledged|storyId={}|actorId={}|ownerId={}|interactionId={}|topic={}|partition={}|offset={}",
                payload.targetId(), payload.actorId(), payload.targetOwnerId(), payload.interactionId(),
                metadata.topic(), metadata.partition(), metadata.offset());
        return Mono.empty();
    }

    private Mono<LikePersistence> persistLike(String storyId, String actorId, String candidateInteractionId) {
        return viewRepository.markLiked(storyId, actorId, candidateInteractionId)
                .flatMap(updated -> {
                    boolean changed = updated != null && updated > 0;
                    return viewRepository.findByStoryIdAndViewerId(storyId, actorId)
                            .flatMap(view -> ensurePersistedLike(view, candidateInteractionId)
                                    .map(persisted -> new LikePersistence(changed, persisted)))
                            .switchIfEmpty(Mono.defer(() -> insertLikedView(
                                            storyId,
                                            actorId,
                                            candidateInteractionId)
                                    .map(inserted -> new LikePersistence(true, inserted))));
                });
    }

    private Mono<StoryView> ensurePersistedLike(StoryView view, String candidateInteractionId) {
        if (!LIKE_REACTION.equalsIgnoreCase(view.getReaction())) {
            return viewRepository.markLiked(view.getStoryId(), view.getViewerId(), candidateInteractionId)
                    .flatMap(updated -> {
                        if (updated == null || updated <= 0) {
                            return Mono.error(new AppException(
                                    ErrorCode.LIKE_CREATE_FAILED,
                                    "Story Like transition could not be persisted"));
                        }
                        return requirePersistedLike(view.getStoryId(), view.getViewerId());
                    });
        }
        if (hasText(view.getReactionInteractionId())) {
            return Mono.just(view);
        }
        return viewRepository.assignLikeInteractionIdIfMissing(
                        view.getStoryId(),
                        view.getViewerId(),
                        candidateInteractionId)
                .then(requirePersistedLike(view.getStoryId(), view.getViewerId()));
    }

    private Mono<StoryView> requirePersistedLike(String storyId, String actorId) {
        return viewRepository.findByStoryIdAndViewerId(storyId, actorId)
                .filter(view -> LIKE_REACTION.equalsIgnoreCase(view.getReaction()))
                .filter(view -> hasText(view.getReactionInteractionId()))
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.LIKE_CREATE_FAILED,
                        "Story Like interaction identity is missing")));
    }

    private Mono<StoryView> insertLikedView(String storyId, String actorId, String interactionId) {
        StoryView view = StoryView.builder()
                .id(UUID.randomUUID().toString())
                .storyId(storyId)
                .viewerId(actorId)
                .reaction(LIKE_REACTION)
                .reactionInteractionId(interactionId)
                .viewedAt(Instant.now())
                .build();
        return entityTemplate.insert(StoryView.class)
                .using(view)
                .onErrorResume(DuplicateKeyException.class,
                        error -> viewRepository.markLiked(storyId, actorId, interactionId)
                                .then(viewRepository.assignLikeInteractionIdIfMissing(
                                        storyId, actorId, interactionId))
                                .then(requirePersistedLike(storyId, actorId)));
    }

    private Mono<UserStories> activeStoryForReaction(String storyId, String actorId) {
        Instant now = Instant.now();
        return storiesRepository.findById(storyId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.STORY_NOT_FOUND, "Story not found")))
                .flatMap(story -> {
                    if (actorId.equals(story.getUserId())) {
                        return Mono.error(new AppException(ErrorCode.LIKE_CREATE_FAILED, "Cannot like your own story"));
                    }
                    if (!"APPROVED".equalsIgnoreCase(story.getStatus()) || !isActive(story, now)) {
                        return Mono.error(new AppException(ErrorCode.STORY_NOT_FOUND, "Story is unavailable"));
                    }
                    return Mono.just(story);
                });
    }

    private boolean isActive(UserStories story, Instant now) {
        if (story.getExpiredAt() != null) {
            return story.getExpiredAt().isAfter(now);
        }
        return story.getCreatedAt() != null && story.getCreatedAt().plusSeconds(24 * 60 * 60).isAfter(now);
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.LIKE_CREATE_FAILED, field + " is required");
        }
        return value.trim();
    }

    private record LikePersistence(boolean changed, StoryView view) {
    }
}
