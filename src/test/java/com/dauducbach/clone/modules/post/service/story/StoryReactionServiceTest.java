package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.post.entity.story.StoryView;
import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repositoty.story.StoryLikeOutboxRepository;
import com.dauducbach.clone.modules.post.repositoty.story.StoryViewRepository;
import com.dauducbach.clone.modules.post.repositoty.story.UserStoriesRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryReactionServiceTest {
    @Mock UserStoriesRepository storiesRepository;
    @Mock StoryViewRepository viewRepository;
    @Mock R2dbcEntityTemplate entityTemplate;
    @Mock StoryLikeOutboxRepository outboxRepository;
    @Mock TransactionalOperator transactionalOperator;
    @Mock ReactiveInsertOperation.ReactiveInsert<StoryView> insertSpec;

    @BeforeEach
    void passThroughTransactions() {
        org.mockito.Mockito.lenient().when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void repeatedPutRepairsTheOutboxWithTheSamePersistedInteraction() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.markLiked(eq("story-1"), eq("actor-1"), anyString()))
                .thenReturn(Mono.just(1), Mono.just(0));
        when(viewRepository.findByStoryIdAndViewerId("story-1", "actor-1"))
                .thenReturn(Mono.just(view("LIKE")));
        when(outboxRepository.enqueue(eq("interaction-1"), eq("story-1"), eq("actor-1"),
                eq("owner-1"), any(Instant.class))).thenReturn(Mono.just(1));
        StoryReactionService service = newService();

        StepVerifier.create(service.like("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();
        StepVerifier.create(service.like("story-1", "actor-1"))
                .expectNext(false)
                .verifyComplete();

        verify(outboxRepository, times(2)).enqueue(eq("interaction-1"), eq("story-1"),
                eq("actor-1"), eq("owner-1"), any(Instant.class));
        verify(transactionalOperator, times(2)).transactional(any(Mono.class));
    }

    @Test
    void outboxFailureFailsTheTransactionInsteadOfReturningSuccess() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.markLiked(eq("story-1"), eq("actor-1"), anyString())).thenReturn(Mono.just(1));
        when(viewRepository.findByStoryIdAndViewerId("story-1", "actor-1"))
                .thenReturn(Mono.just(view("LIKE")));
        when(outboxRepository.enqueue(anyString(), eq("story-1"), eq("actor-1"),
                eq("owner-1"), any(Instant.class)))
                .thenReturn(Mono.error(new IllegalStateException("outbox unavailable")));

        StepVerifier.create(newService().like("story-1", "actor-1"))
                .expectErrorMessage("outbox unavailable")
                .verify();

        verify(transactionalOperator).transactional(any(Mono.class));
    }

    @Test
    void createsTheViewAndOutboxWhenTheViewerHasNotOpenedTheStoryYet() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.markLiked(eq("story-1"), eq("actor-1"), anyString())).thenReturn(Mono.just(0));
        when(viewRepository.findByStoryIdAndViewerId("story-1", "actor-1")).thenReturn(Mono.empty());
        when(entityTemplate.insert(StoryView.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(StoryView.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(outboxRepository.enqueue(anyString(), eq("story-1"), eq("actor-1"),
                eq("owner-1"), any(Instant.class))).thenReturn(Mono.just(1));

        StepVerifier.create(newService().like("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();

        verify(insertSpec).using(any(StoryView.class));
        verify(outboxRepository).enqueue(anyString(), eq("story-1"), eq("actor-1"),
                eq("owner-1"), any(Instant.class));
    }

    @Test
    void unlikeDoesNotDeleteAnAlreadyDurableOutboxEvent() {
        when(storiesRepository.findById("story-1")).thenReturn(Mono.just(activeStory()));
        when(viewRepository.clearLike("story-1", "actor-1")).thenReturn(Mono.just(1));

        StepVerifier.create(newService().unlike("story-1", "actor-1"))
                .expectNext(true)
                .verifyComplete();

        verify(outboxRepository, never()).acknowledge(anyString(), anyString());
    }

    private StoryReactionService newService() {
        return new StoryReactionService(
                storiesRepository,
                viewRepository,
                entityTemplate,
                outboxRepository,
                transactionalOperator);
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
                .reactionInteractionId("interaction-1")
                .viewedAt(Instant.now())
                .build();
    }
}
