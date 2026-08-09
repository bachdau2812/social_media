package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repositoty.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import com.dauducbach.clone.utils.GsonUtils;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryReactionServiceTest {
    @Mock UserStoriesRepository stories;
    @Mock StoryViewRepository views;
    @Mock R2dbcEntityTemplate template;
    @Mock KafkaSender<String, String> sender;
    @Mock SenderResult<String> result;
    @Mock RecordMetadata metadata;
    @Mock ReactiveInsertOperation.ReactiveInsert<StoryView> insertSpec;

    @Test void firstLikePublishesDirectly() {
        stubPersistence(1);
        stubSuccess();
        StepVerifier.create(service().like("story-1", "actor-1")).expectNext(true).verifyComplete();
        verify(sender).send(any(Publisher.class));
    }

    @Test void repeatedPutPublishesThePersistedInteractionId() {
        stubStory();
        when(views.markLiked(eq("story-1"), eq("actor-1"), anyString()))
                .thenReturn(Mono.just(1), Mono.just(0));
        when(views.findByStoryIdAndViewerId("story-1", "actor-1")).thenReturn(Mono.just(view()));
        stubSuccess();
        StoryReactionService service = service();
        StepVerifier.create(service.like("story-1", "actor-1")).expectNext(true).verifyComplete();
        StepVerifier.create(service.like("story-1", "actor-1")).expectNext(false).verifyComplete();
        verify(sender, times(2)).send(any(Publisher.class));
    }
    @Test void publisherFailurePropagatesTheOriginalError() {
        stubPersistence(1);
        IllegalStateException failure = new IllegalStateException();
        when(sender.send(any())).thenReturn(Flux.error(failure));
        StepVerifier.create(service().like("story-1", "actor-1"))
                .expectErrorMatches(failure::equals).verify();
    }

    @Test void senderResultFailurePropagatesTheOriginalError() {
        IllegalStateException failure = new IllegalStateException("broker rejected record");
        stubPersistence(1);
        when(sender.send(any(Publisher.class))).thenReturn(Flux.just(result));
        when(result.exception()).thenReturn(failure);

        StepVerifier.create(service().like("story-1", "actor-1"))
                .expectErrorMatches(failure::equals).verify();
    }

    @Test void missingBrokerMetadataFailsTheLikeRequest() {
        stubPersistence(1);
        when(sender.send(any(Publisher.class))).thenReturn(Flux.just(result));
        when(result.exception()).thenReturn(null);
        when(result.recordMetadata()).thenReturn(null);

        StepVerifier.create(service().like("story-1", "actor-1"))
                .expectErrorMessage("Kafka acknowledgement metadata is missing").verify();
    }

    @Test void createsTheViewAndPublishesWhenTheViewerHasNotOpenedTheStory() {
        stubStory();
        when(views.markLiked(eq("story-1"), eq("actor-1"), anyString())).thenReturn(Mono.just(0));
        when(views.findByStoryIdAndViewerId("story-1", "actor-1")).thenReturn(Mono.empty());
        when(template.insert(StoryView.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(StoryView.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
        stubSuccess(null);

        StepVerifier.create(service().like("story-1", "actor-1")).expectNext(true).verifyComplete();

        verify(insertSpec).using(any(StoryView.class));
        verify(sender).send(any(Publisher.class));
    }

    @Test void unlikeDoesNotPublishALikeEvent() {
        stubStory();
        when(views.clearLike("story-1", "actor-1")).thenReturn(Mono.just(1));

        StepVerifier.create(service().unlike("story-1", "actor-1")).expectNext(true).verifyComplete();

        verify(sender, never()).send(any(Publisher.class));
    }

    private StoryReactionService service() {
        return new StoryReactionService(stories, views, template, sender);
    }

    private void stubPersistence(int changed) {
        stubStory();
        when(views.markLiked(eq("story-1"), eq("actor-1"), anyString())).thenReturn(Mono.just(changed));
        when(views.findByStoryIdAndViewerId("story-1", "actor-1")).thenReturn(Mono.just(view()));
    }

    private void stubStory() {
        when(stories.findById("story-1")).thenReturn(Mono.just(UserStories.builder()
                .id("story-1").userId("owner-1").status("APPROVED")
                .createdAt(Instant.now().minusSeconds(60)).expiredAt(Instant.now().plusSeconds(3600)).build()));
    }

    private void stubSuccess() {
        stubSuccess("interaction-1");
    }

    private void stubSuccess(String expectedInteractionId) {
        when(sender.send(any())).thenAnswer(call -> Flux.from(call.<Publisher<SenderRecord<String,String,String>>>getArgument(0))
                .doOnNext(record -> assertEvent(record, expectedInteractionId)).map(ignored -> result));
        when(result.exception()).thenReturn(null);
        when(result.recordMetadata()).thenReturn(metadata);
        when(metadata.topic()).thenReturn("like_event");
        when(metadata.partition()).thenReturn(0);
        when(metadata.offset()).thenReturn(104L);
    }

    private void assertEvent(SenderRecord<String,String,String> record, String expectedInteractionId) {
        var json = GsonUtils.fromString(record.value());
        assertThat(record.topic()).isEqualTo("like_event");
        assertThat(record.key()).isEqualTo("story-1");
        assertThat(json.get("actorId").getAsString()).isEqualTo("actor-1");
        assertThat(json.get("targetId").getAsString()).isEqualTo("story-1");
        assertThat(json.get("targetType").getAsString()).isEqualTo("STORY");
        assertThat(json.get("targetOwnerId").getAsString()).isEqualTo("owner-1");
        assertThat(json.get("interactionId").getAsString()).isEqualTo(record.correlationMetadata());
        assertThat(json.has("timestamp")).isTrue();
        assertThatCode(() -> Instant.parse(json.get("timestamp").getAsString()))
                .doesNotThrowAnyException();
        if (expectedInteractionId != null) {
            assertThat(record.correlationMetadata()).isEqualTo(expectedInteractionId);
        }
    }

    private StoryView view() {
        return StoryView.builder().id("view-1").storyId("story-1").viewerId("actor-1")
                .reaction("LIKE").reactionInteractionId("interaction-1").viewedAt(Instant.now()).build();
    }
}
