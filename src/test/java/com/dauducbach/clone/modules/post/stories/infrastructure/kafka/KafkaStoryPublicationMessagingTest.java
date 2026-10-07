package com.dauducbach.clone.modules.post.stories.infrastructure.kafka;

import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.concurrent.atomic.AtomicReference;

class KafkaStoryPublicationMessagingTest {
    @Test
    void scanRequestKeepsTheExistingKafkaTopicAndPayloadShape() {
        KafkaSender<String, String> kafkaSender = mock(KafkaSender.class);
        PostSseService sseService = mock(PostSseService.class);
        when(kafkaSender.send(any(Publisher.class))).thenReturn(Flux.empty());
        KafkaStoryPublicationMessaging messaging = new KafkaStoryPublicationMessaging(kafkaSender, sseService);

        UserStories story = story();
        messaging.requestMediaScan(story, "stories/story_1").block();

        JsonObject payload = capturedPayload(kafkaSender, "check_story_media_event");
        assertThat(payload.get("storyId").getAsString()).isEqualTo("story-1");
        assertThat(payload.get("userId").getAsString()).isEqualTo("owner-1");
        assertThat(payload.get("mediaUrl").getAsString()).isEqualTo("https://cdn.example/story.jpg");
        assertThat(payload.get("mediaType").getAsString()).isEqualTo("IMAGE");
        assertThat(payload.get("musicId").getAsString()).isEqualTo("music-1");
        assertThat(payload.get("musicStart").getAsLong()).isEqualTo(5L);
        assertThat(payload.get("musicEnd").getAsLong()).isEqualTo(25L);
        assertThat(payload.get("publicationId").getAsString()).isEqualTo("publication-1");
        assertThat(payload.get("publicationOrder").getAsInt()).isEqualTo(2);
        assertThat(payload.get("publicationItemCount").getAsInt()).isEqualTo(3);
        assertThat(payload.get("publicId").getAsString()).isEqualTo("stories/story_1");
    }

    @Test
    void approvalKeepsExistingSseAndSuccessEventFields() {
        KafkaSender<String, String> kafkaSender = mock(KafkaSender.class);
        PostSseService sseService = mock(PostSseService.class);
        when(kafkaSender.send(any(Publisher.class))).thenReturn(Flux.empty());
        when(sseService.sendToUser(eq("owner-1"), eq("story_upload_event"), anyString()))
                .thenReturn(Mono.empty());
        KafkaStoryPublicationMessaging messaging = new KafkaStoryPublicationMessaging(kafkaSender, sseService);
        UserStories story = story();
        MediaAssetView media = new MediaAssetView("media-1", "stories/story_1", 0, 0,
                null, "image", 0, null, story.getMediaUrl(), "owner-1", OwnerType.STORY,
                null, null, null, null, null);

        messaging.publishApproved(story, media, "https://cdn.example/music-cut.mp3").block();

        JsonObject event = capturedPayload(kafkaSender, "story_success_event");
        assertThat(event.get("storyId").getAsString()).isEqualTo("story-1");
        assertThat(event.get("mediaId").getAsString()).isEqualTo("media-1");
        assertThat(event.get("musicTransformedUrl").getAsString()).isEqualTo("https://cdn.example/music-cut.mp3");

        ArgumentCaptor<String> sse = ArgumentCaptor.forClass(String.class);
        verify(sseService).sendToUser(eq("owner-1"), eq("story_upload_event"), sse.capture());
        JsonObject status = com.dauducbach.clone.commons.serialization.GsonUtils.fromString(sse.getValue());
        assertThat(status.get("entityId").getAsString()).isEqualTo("story-1");
        assertThat(status.get("ownerType").getAsString()).isEqualTo("STORY");
        assertThat(status.get("result").getAsString()).isEqualTo("APPROVED");
        assertThat(status.get("musicTransformedUrl").getAsString()).isEqualTo("https://cdn.example/music-cut.mp3");
    }

    @SuppressWarnings("unchecked")
    private JsonObject capturedPayload(KafkaSender<String, String> kafkaSender, String expectedTopic) {
        ArgumentCaptor<Publisher<SenderRecord<String, String, String>>> captor =
                ArgumentCaptor.forClass(Publisher.class);
        verify(kafkaSender).send(captor.capture());
        AtomicReference<JsonObject> payload = new AtomicReference<>();
        StepVerifier.create(Flux.from(captor.getValue()))
                .assertNext(record -> {
                    assertThat(record.key()).isEqualTo("story-1");
                    assertThat(record.topic()).isEqualTo(expectedTopic);
                    payload.set(com.dauducbach.clone.commons.serialization.GsonUtils.fromString(record.value()));
                })
                .verifyComplete();
        return payload.get();
    }

    private UserStories story() {
        return UserStories.builder()
                .id("story-1")
                .userId("owner-1")
                .mediaUrl("https://cdn.example/story.jpg")
                .mediaType("IMAGE")
                .musicId("music-1")
                .musicUrl("https://cdn.example/music.mp3")
                .musicStart(5L)
                .musicEnd(25L)
                .publicationId("publication-1")
                .publicationOrder(2)
                .publicationItemCount(3)
                .status("PENDING_SCAN")
                .build();
    }
}
