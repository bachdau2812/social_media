package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.projection.FriendFeedActivityProjection;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostFeedQueryServiceTest {
    @Test
    void friendFeedActivitiesPreserveDistinctRepostsOfTheSamePost() {
        PostDetailsRepository repository = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        PostFeedQueryService service = new PostFeedQueryService(repository, elasticsearch);
        Instant firstAt = Instant.parse("2026-07-31T00:00:00Z");
        Instant secondAt = firstAt.minusSeconds(60);

        when(repository.findRecentFriendFeedActivities("viewer-1", 21, 0)).thenReturn(Flux.just(
                activity("repost-1", "post-1", "REPOST", "friend-1", firstAt),
                activity("repost-2", "post-1", "REPOST", "friend-2", secondAt)
        ));

        StepVerifier.create(service.getRecentFriendFeedActivities("viewer-1", 21, 0))
                .expectNextMatches(activity -> activity.feedEntryId().equals("repost-1")
                        && activity.postId().equals("post-1"))
                .expectNextMatches(activity -> activity.feedEntryId().equals("repost-2")
                        && activity.postId().equals("post-1"))
                .verifyComplete();
    }

    @Test
    void approvedLookupUsesSourceQueryThatExcludesArchivedPosts() {
        PostDetailsRepository repository = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        PostFeedQueryService service = new PostFeedQueryService(repository, elasticsearch);
        PostDetails post = PostDetails.builder()
                .postId("post-1")
                .validateStatus("APPROVED")
                .build();

        when(repository.findApprovedFeedEligibleById("post-1")).thenReturn(Mono.just(post));

        StepVerifier.create(service.getApprovedPostById("post-1"))
                .expectNext(post)
                .verifyComplete();

        verify(repository).findApprovedFeedEligibleById("post-1");
    }

    private FriendFeedActivityProjection activity(
            String feedEntryId,
            String postId,
            String activityType,
            String actorId,
            Instant activityAt
    ) {
        return new FriendFeedActivityProjection() {
            @Override public String getFeedEntryId() { return feedEntryId; }
            @Override public String getPostId() { return postId; }
            @Override public String getActivityType() { return activityType; }
            @Override public String getActorId() { return actorId; }
            @Override public Instant getActivityAt() { return activityAt; }
        };
    }
}

