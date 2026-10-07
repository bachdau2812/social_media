package com.dauducbach.clone.modules.post.stories.infrastructure.kafka;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import com.dauducbach.clone.modules.post.stories.publishing.StoryPublicationMessaging;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Component
public class KafkaStoryPublicationMessaging implements StoryPublicationMessaging {
    private static final Logger log = LoggerFactory.getLogger(KafkaStoryPublicationMessaging.class);

    private final KafkaSender<String, String> kafkaSender;
    private final PostSseService postSseService;

    public KafkaStoryPublicationMessaging(
            KafkaSender<String, String> kafkaSender,
            PostSseService postSseService
    ) {
        this.kafkaSender = kafkaSender;
        this.postSseService = postSseService;
    }

    @Override
    public Mono<Void> requestMediaScan(UserStories story, String publicId) {
        JsonObject payload = storyPayload(story);
        payload.addProperty("publicId", publicId);
        return send("check_story_media_event", story.getId(), payload.toString())
                .doOnError(error -> log.error("|KafkaStoryPublicationMessaging|requestMediaScan|storyId={}|error={}",
                        story.getId(), error.getMessage()));
    }

    @Override
    public Mono<Void> publishApproved(UserStories story, MediaAssetView media, String transformedMusicUrl) {
        JsonObject ssePayload = baseStatusPayload(story, "APPROVED", "Story approved");
        ssePayload.addProperty("mediaId", media.assetId());
        ssePayload.addProperty("musicId", story.getMusicId());
        ssePayload.addProperty("musicUrl", story.getMusicUrl());
        ssePayload.addProperty("musicStart", story.getMusicStart());
        ssePayload.addProperty("musicEnd", story.getMusicEnd());
        ssePayload.addProperty("musicTransformedUrl", transformedMusicUrl);

        JsonObject eventPayload = storyPayload(story);
        eventPayload.addProperty("musicTransformedUrl", transformedMusicUrl);
        eventPayload.addProperty("mediaId", media.assetId());
        return postSseService.sendToUser(story.getUserId(), "story_upload_event", ssePayload.toString())
                .then(send("story_success_event", story.getId(), eventPayload.toString()))
                .doOnError(error -> log.error("|KafkaStoryPublicationMessaging|publishApproved|storyId={}|error={}",
                        story.getId(), error.getMessage()));
    }

    @Override
    public Mono<Void> publishRejected(String userId, String storyId, String mediaUrl, String message) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", userId);
        payload.addProperty("entityId", storyId);
        payload.addProperty("ownerType", OwnerType.STORY.name());
        payload.addProperty("mediaUrl", mediaUrl);
        payload.addProperty("result", "REJECTED");
        payload.addProperty("message", message);
        return postSseService.sendToUser(userId, "story_upload_event", payload.toString());
    }

    private Mono<Void> send(String topic, String key, String payload) {
        SenderRecord<String, String, String> record = SenderRecord.create(
                new ProducerRecord<>(topic, key, payload), topic);
        return kafkaSender.send(Mono.just(record))
                .flatMap(result -> result.exception() == null ? Mono.just(result) : Mono.error(result.exception()))
                .then();
    }

    private JsonObject storyPayload(UserStories story) {
        JsonObject payload = new JsonObject();
        payload.addProperty("storyId", story.getId());
        payload.addProperty("userId", story.getUserId());
        payload.addProperty("mediaUrl", story.getMediaUrl());
        payload.addProperty("mediaType", story.getMediaType());
        payload.addProperty("musicId", story.getMusicId());
        payload.addProperty("musicUrl", story.getMusicUrl());
        payload.addProperty("musicStart", story.getMusicStart());
        payload.addProperty("musicEnd", story.getMusicEnd());
        payload.addProperty("publicationId", story.getPublicationId());
        payload.addProperty("publicationOrder", story.getPublicationOrder());
        payload.addProperty("publicationItemCount", story.getPublicationItemCount());
        return payload;
    }

    private JsonObject baseStatusPayload(UserStories story, String status, String message) {
        JsonObject payload = new JsonObject();
        payload.addProperty("userId", story.getUserId());
        payload.addProperty("entityId", story.getId());
        payload.addProperty("ownerType", OwnerType.STORY.name());
        payload.addProperty("mediaUrl", story.getMediaUrl());
        payload.addProperty("result", status);
        payload.addProperty("message", message);
        return payload;
    }
}
