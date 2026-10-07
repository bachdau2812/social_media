package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.projection.FriendFeedActivityProjection;
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
    @Test void infrastructureFailureMustRemainRetryable() {
        PostDetailsRepository repository = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        RuntimeException failure = new RuntimeException("ES unavailable");
        when(repository.findById("p")).thenReturn(Mono.just(PostDetails.builder().postId("p").validateStatus("APPROVED").build()));
        when(elasticsearch.get("p", com.dauducbach.clone.modules.post.elastic.PostVector.class)).thenReturn(Mono.error(failure));
        StepVerifier.create(service(repository, elasticsearch).getPostVector("p"))
                .expectErrorMatches(error -> error == failure).verify();
    }
    @Test void approvedMissingVectorCannotBeAcknowledgedAsEmpty() {
        PostDetailsRepository repository = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        when(repository.findById("p")).thenReturn(Mono.just(PostDetails.builder().postId("p").validateStatus("APPROVED").build()));
        when(elasticsearch.get("p", com.dauducbach.clone.modules.post.elastic.PostVector.class)).thenReturn(Mono.empty());
        StepVerifier.create(service(repository, elasticsearch).getPostVector("p"))
                .expectError().verify();
    }
    @Test
    void friendFeedActivitiesPreserveDistinctRepostsOfTheSamePost() {
        PostDetailsRepository repository = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        PostFeedQueryService service = service(repository, elasticsearch);
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
        PostFeedQueryService service = service(repository, elasticsearch);
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

    private PostFeedQueryService service(PostDetailsRepository posts, ReactiveElasticsearchOperations es) {
        PostItemRepository items = mock(PostItemRepository.class);
        when(items.findByPostIdOrderByOrderNumberAsc(org.mockito.ArgumentMatchers.anyString())).thenReturn(Flux.empty());
        return new PostFeedQueryService(posts, es, new PostEmbeddingSourceReader(posts, items, new PostEmbeddingTextBuilder()));
    }

    @Test void getterUsesRecommendationWithKnownModelContentFallback() {
        PostDetailsRepository posts = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations es = mock(ReactiveElasticsearchOperations.class);
        PostDetails source = PostVectorServiceTest.post("text");
        when(posts.findById("p")).thenReturn(Mono.just(source));
        PostVector doc = PostVector.builder().postId("p").model("gemini-embedding-2").dimension(768).schemaVersion(1)
                .embeddingState("READY").sourceRevision(new PostEmbeddingTextBuilder().revision(source, java.util.List.of()))
                .contentVector(PostVectorServiceTest.axis(0)).recommendationVector(PostVectorServiceTest.axis(1)).build();
        when(es.get("p", PostVector.class)).thenReturn(Mono.just(doc));
        PostFeedQueryService service = service(posts, es);
        org.springframework.test.util.ReflectionTestUtils.setField(service, "recommendationEnabled", true);
        StepVerifier.create(service.getRecommendationVector("p")).expectNext(PostVectorServiceTest.axis(1)).verifyComplete();
        doc.setRecommendationVector(null);
        StepVerifier.create(service(posts, es).getPostVector("p")).expectNext(PostVectorServiceTest.axis(0)).verifyComplete();
    }
    @Test void pendingModerationAndUnknownLegacyAreRetryableRatherThanPermanentEmpty() {
        PostDetailsRepository posts = mock(PostDetailsRepository.class); ReactiveElasticsearchOperations es = mock(ReactiveElasticsearchOperations.class);
        PostDetails source = PostVectorServiceTest.post("text"); source.setValidateStatus("PENDING_SCAN");
        when(posts.findById("p")).thenReturn(Mono.just(source));
        when(es.get("p", PostVector.class)).thenReturn(Mono.just(PostVector.builder().contentVector(PostVectorServiceTest.axis(0)).build()));
        StepVerifier.create(service(posts, es).getPostVector("p")).expectError(PostVectorPendingException.class).verify();
        source.setValidateStatus("APPROVED");
        StepVerifier.create(service(posts, es).getPostVector("p")).expectError(PostVectorPendingException.class).verify();
    }
    @Test void staleReadyOrSkipStateCannotLoseAnInteraction() {
        PostDetailsRepository posts = mock(PostDetailsRepository.class); ReactiveElasticsearchOperations es = mock(ReactiveElasticsearchOperations.class);
        when(posts.findById("p")).thenReturn(Mono.just(PostVectorServiceTest.post("changed text")));
        PostVector doc = PostVector.builder().postId("p").sourceRevision("old").embeddingState("SKIPPED_NO_INPUT").build();
        when(es.get("p", PostVector.class)).thenReturn(Mono.just(doc));
        StepVerifier.create(service(posts, es).getPostVector("p")).expectError(PostVectorPendingException.class).verify();
        doc.setEmbeddingState("READY"); doc.setContentVector(PostVectorServiceTest.axis(0));
        StepVerifier.create(service(posts, es).getPostVector("p")).expectError(PostVectorPendingException.class).verify();
    }
    @Test void permanentDeletedRejectedAndCurrentNoInputReturnExplicitEmpty() {
        PostDetailsRepository posts = mock(PostDetailsRepository.class); ReactiveElasticsearchOperations es = mock(ReactiveElasticsearchOperations.class);
        when(posts.findById("p")).thenReturn(Mono.empty()); when(es.get("p", PostVector.class)).thenReturn(Mono.empty());
        StepVerifier.create(service(posts, es).getPostVector("p")).expectNext(java.util.List.of()).verifyComplete();
        PostDetails source = PostVectorServiceTest.post("text"); source.setValidateStatus("REJECTED");
        when(posts.findById("p")).thenReturn(Mono.just(source));
        StepVerifier.create(service(posts, es).getPostVector("p")).expectNext(java.util.List.of()).verifyComplete();
        source.setValidateStatus("APPROVED");
        PostVector doc = PostVector.builder().postId("p").sourceRevision(new PostEmbeddingTextBuilder().revision(source, java.util.List.of()))
                .embeddingState("SKIPPED_NO_INPUT").model("gemini-embedding-2").dimension(768).schemaVersion(1).build();
        when(es.get("p", PostVector.class)).thenReturn(Mono.just(doc));
        StepVerifier.create(service(posts, es).getPostVector("p")).expectNext(java.util.List.of()).verifyComplete();
        doc.setDeleted(true);
        StepVerifier.create(service(posts, es).getPostVector("p")).expectNext(java.util.List.of()).verifyComplete();
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
