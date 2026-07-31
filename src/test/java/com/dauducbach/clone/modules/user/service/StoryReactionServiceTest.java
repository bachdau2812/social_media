package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.modules.user.entity.StoryView;
import com.dauducbach.clone.modules.user.entity.UserStories;
import com.dauducbach.clone.modules.user.repositoty.StoryViewRepository;
import com.dauducbach.clone.modules.user.repositoty.UserStoriesRepository;
import com.google.gson.JsonObject;
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
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryReactionServiceTest {
    @Mock UserStoriesRepository storiesRepository;
    @Mock StoryViewRepository viewRepository;
    @Mock KafkaSender<String, String> kafkaSender;
    @Mock R2dbcEntityTemplate entityTemplate;
    @Mock ReactiveInsertOperation.ReactiveInsert<StoryView> insertSpec;

    @Test
    void publishesOnceOnlyWhenLikeChangesThePersistedReaction() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.markLiked("story-1", "actor-1"))
                .thenReturn(Mono.just(1), Mono.just(0));
        when(viewRepository.findByStoryIdAndViewerId("story-1", "actor-1"))
                .thenReturn(Mono.just(view("LIKE")));
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> publisher = invocation.getArgument(0);
            return Flux.from(publisher)
                    .doOnNext(record -> {
                        assertThat(record.topic()).isEqualTo("like_event");
                        assertThat(record.key()).isEqualTo("story-1");
                        JsonObject payload = GsonUtils.fromString(record.value());
                        assertThat(payload.get("actorId").getAsString()).isEqualTo("actor-1");
                        assertThat(payload.get("targetId").getAsString()).isEqualTo("story-1");
                        assertThat(payload.get("targetType").getAsString()).isEqualTo("STORY");
                        assertThat(payload.get("targetOwnerId").getAsString()).isEqualTo("owner-1");
                        assertThat(payload.get("interactionId").getAsString()).isNotBlank();
                    })
                    .thenMany(Flux.empty());
        });
        StoryReactionService service = newService();

        StepVerifier.create(service.like("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();
        StepVerifier.create(service.like("story-1", "actor-1"))
                .expectNext(false)
                .verifyComplete();

        verify(kafkaSender, times(1)).send(any(Publisher.class));
    }

    @Test
    void createsTheViewRowWhenTheViewerHasNotOpenedTheStoryYet() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.markLiked("story-1", "actor-1")).thenReturn(Mono.just(0));
        when(viewRepository.findByStoryIdAndViewerId("story-1", "actor-1")).thenReturn(Mono.empty());
        when(entityTemplate.insert(StoryView.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(StoryView.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(kafkaSender.send(any(Publisher.class))).thenReturn(Flux.empty());

        StepVerifier.create(newService().like("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();

        verify(insertSpec).using(any(StoryView.class));
        verify(kafkaSender).send(any(Publisher.class));
    }

    @Test
    void unlikeIsIdempotentAndRelikeEmitsANewNotificationEvent() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.clearLike("story-1", "actor-1"))
                .thenReturn(Mono.just(1), Mono.just(0));
        StoryReactionService service = newService();

        StepVerifier.create(service.unlike("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();
        StepVerifier.create(service.unlike("story-1", "actor-1"))
                .expectNext(false)
                .verifyComplete();

        verify(kafkaSender, never()).send(any(Publisher.class));

        when(viewRepository.markLiked("story-1", "actor-1")).thenReturn(Mono.just(1));
        when(kafkaSender.send(any(Publisher.class))).thenReturn(Flux.empty());
        StepVerifier.create(service.like("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();
        verify(kafkaSender, times(1)).send(any(Publisher.class));
    }

    private StoryReactionService newService() {
        return new StoryReactionService(storiesRepository, viewRepository, kafkaSender, entityTemplate);
    }

    private UserStories activeStory() {
        Instant now = Instant.now();
        return UserStories.builder()
                .id("story-1")
                .userId("owner-1")
                .status("APPROVED")
                .createdAt(now.minusSeconds(60))
                .expiredAt(now.plusSeconds(3_600))
                .build();
    }

    private StoryView view(String reaction) {
        return StoryView.builder()
                .id("view-1")
                .storyId("story-1")
                .viewerId("actor-1")
                .reaction(reaction)
                .viewedAt(Instant.now())
                .build();
    }
}
