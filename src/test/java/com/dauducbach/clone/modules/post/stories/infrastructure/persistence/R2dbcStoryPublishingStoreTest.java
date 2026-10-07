package com.dauducbach.clone.modules.post.stories.infrastructure.persistence;

import com.dauducbach.clone.modules.post.entity.story.UserStories;
import com.dauducbach.clone.modules.post.repository.story.UserStoriesRepository;
import com.dauducbach.clone.modules.post.stories.publishing.StoryPublishingStore.Submission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class R2dbcStoryPublishingStoreTest {
    @Mock
    UserStoriesRepository userStoriesRepository;
    @Mock
    R2dbcEntityTemplate entityTemplate;

    @Test
    void insertsNewStoryAndMarksItForScanning() {
        UserStories candidate = story("story-1", "publication-1", 1);
        ReactiveInsertOperation.ReactiveInsert<UserStories> insert = mock(ReactiveInsertOperation.ReactiveInsert.class);
        when(entityTemplate.insert(UserStories.class)).thenReturn(insert);
        when(insert.using(candidate)).thenReturn(Mono.just(candidate));
        R2dbcStoryPublishingStore store = new R2dbcStoryPublishingStore(userStoriesRepository, entityTemplate);

        StepVerifier.create(store.createOrReuse(candidate, false))
                .assertNext(result -> assertSubmission(result, candidate, true))
                .verifyComplete();
        verify(insert).using(candidate);
    }

    @Test
    void recoversUniquePublicationRaceByReturningTheExistingStoryWithoutRescan() {
        UserStories candidate = story("new-id", "publication-1", 1);
        UserStories existing = story("existing-id", "publication-1", 1);
        ReactiveInsertOperation.ReactiveInsert<UserStories> insert = mock(ReactiveInsertOperation.ReactiveInsert.class);
        when(userStoriesRepository.findByUserIdAndPublicationIdAndPublicationOrder("owner-1", "publication-1", 1))
                .thenReturn(Mono.empty(), Mono.just(existing));
        when(entityTemplate.insert(UserStories.class)).thenReturn(insert);
        when(insert.using(candidate)).thenReturn(Mono.error(new DataIntegrityViolationException("duplicate publication")));
        R2dbcStoryPublishingStore store = new R2dbcStoryPublishingStore(userStoriesRepository, entityTemplate);

        StepVerifier.create(store.createOrReuse(candidate, true))
                .assertNext(result -> assertSubmission(result, existing, false))
                .verifyComplete();
        verify(userStoriesRepository, org.mockito.Mockito.times(2))
                .findByUserIdAndPublicationIdAndPublicationOrder("owner-1", "publication-1", 1);
    }

    private void assertSubmission(Submission actual, UserStories expected, boolean shouldScan) {
        org.assertj.core.api.Assertions.assertThat(actual.story().getId()).isEqualTo(expected.getId());
        org.assertj.core.api.Assertions.assertThat(actual.shouldScan()).isEqualTo(shouldScan);
    }

    private UserStories story(String id, String publicationId, int order) {
        return UserStories.builder()
                .id(id)
                .userId("owner-1")
                .publicationId(publicationId)
                .publicationOrder(order)
                .publicationItemCount(2)
                .status("PENDING_SCAN")
                .build();
    }
}
