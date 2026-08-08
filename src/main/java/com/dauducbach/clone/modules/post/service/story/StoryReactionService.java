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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class StoryReactionService {
    private static final String LIKE_EVENT_TOPIC = "like_event";
    private static final String LIKE_REACTION = "LIKE";

    private final UserStoriesRepository storiesRepository;
    private final StoryViewRepository viewRepository;
    private final KafkaSender<String, String> kafkaSender;
    private final R2dbcEntityTemplate entityTemplate;

    public Mono<Boolean> like(String storyId, String actorId) {
        String normalizedStoryId = requireText(storyId, "storyId");
        String normalizedActorId = requireText(actorId, "actorId");
        return activeStoryForReaction(normalizedStoryId, normalizedActorId)
                .flatMap(story -> markLiked(story, normalizedActorId)
                        .flatMap(changed -> changed
                                ? publishLikeEvent(story, normalizedActorId).thenReturn(true)
                                : Mono.just(false)));
    }

    public Mono<Boolean> unlike(String storyId, String actorId) {
        String normalizedStoryId = requireText(storyId, "storyId");
        String normalizedActorId = requireText(actorId, "actorId");
        return activeStoryForReaction(normalizedStoryId, normalizedActorId)
                .flatMap(story -> viewRepository.clearLike(story.getId(), normalizedActorId))
                .map(updated -> updated != null && updated > 0);
    }

    private Mono<Boolean> markLiked(UserStories story, String actorId) {
        return viewRepository.markLiked(story.getId(), actorId)
                .flatMap(updated -> {
                    if (updated != null && updated > 0) {
                        return Mono.just(true);
                    }
                    return viewRepository.findByStoryIdAndViewerId(story.getId(), actorId)
                            .map(existing -> false)
                            .switchIfEmpty(Mono.defer(() -> insertLikedView(story.getId(), actorId)));
                });
    }

    private Mono<Boolean> insertLikedView(String storyId, String actorId) {
        StoryView view = StoryView.builder()
                .id(UUID.randomUUID().toString())
                .storyId(storyId)
                .viewerId(actorId)
                .reaction(LIKE_REACTION)
                .viewedAt(Instant.now())
                .build();
        return entityTemplate.insert(StoryView.class)
                .using(view)
                .thenReturn(true)
                .onErrorResume(DuplicateKeyException.class,
                        error -> viewRepository.markLiked(storyId, actorId)
                                .map(updated -> updated != null && updated > 0));
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

    private Mono<Void> publishLikeEvent(UserStories story, String actorId) {
        StoryLikeEventPayload payload = new StoryLikeEventPayload(
                actorId,
                story.getId(),
                "STORY",
                story.getUserId(),
                UUID.randomUUID().toString(),
                Instant.now()
        );
        JsonObject event = new JsonObject();
        event.addProperty("actorId", payload.actorId());
        event.addProperty("targetId", payload.targetId());
        event.addProperty("targetType", payload.targetType());
        event.addProperty("targetOwnerId", payload.targetOwnerId());
        event.addProperty("interactionId", payload.interactionId());
        event.addProperty("timestamp", payload.timestamp().toString());
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(LIKE_EVENT_TOPIC, story.getId(), event.toString()),
                LIKE_EVENT_TOPIC
        );
        return kafkaSender.send(Mono.just(record)).then();
    }

    private String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.LIKE_CREATE_FAILED, field + " is required");
        }
        return value.trim();
    }
}
