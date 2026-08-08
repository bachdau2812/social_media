package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FeedCandidatePipelineTest {

    @Test
    void vectorCandidatesLeadRecentCandidatesAndDuplicatesKeepFirstSource() {
        FeedVectorService vectorService = mock(FeedVectorService.class);
        PostFeedQueryService postQuery = mock(PostFeedQueryService.class);
        FeedCandidatePipeline pipeline = new FeedCandidatePipeline(vectorService, postQuery);

        when(vectorService.buildQueryVector("user-1")).thenReturn(Mono.just(List.of(0.2, 0.8)));
        when(postQuery.searchRecommendedPostIds(List.of(0.2, 0.8), 10, Set.of("seen")))
                .thenReturn(Mono.just(List.of("vector-1", "duplicate")));
        when(postQuery.getRecentApprovedPosts(10, Set.of("seen")))
                .thenReturn(Flux.just(post("duplicate"), post("recent-1")));

        StepVerifier.create(pipeline.select("user-1", 10, Set.of("seen")))
                .assertNext(candidates -> {
                    assertThat(candidates).extracting(FeedCandidate::postId)
                            .containsExactly("vector-1", "duplicate", "recent-1");
                    assertThat(candidates).extracting(FeedCandidate::sourceType)
                            .containsExactly("vector", "vector", "recent");
                    assertThat(candidates).extracting(FeedCandidate::deliveryScore)
                            .containsExactly(3.0, 2.0, 1.0);
                })
                .verifyComplete();
    }

    @Test
    void recentSourceRemainsAvailableWhenVectorSourceFails() {
        FeedVectorService vectorService = mock(FeedVectorService.class);
        PostFeedQueryService postQuery = mock(PostFeedQueryService.class);
        FeedCandidatePipeline pipeline = new FeedCandidatePipeline(vectorService, postQuery);

        when(vectorService.buildQueryVector("user-1")).thenReturn(Mono.error(new IllegalStateException("vector unavailable")));
        when(postQuery.getRecentApprovedPosts(5, Set.of())).thenReturn(Flux.just(post("recent-1")));

        StepVerifier.create(pipeline.select("user-1", 5, Set.of()))
                .assertNext(candidates -> {
                    assertThat(candidates).hasSize(1);
                    assertThat(candidates.getFirst().sourceType()).isEqualTo("recent");
                })
                .verifyComplete();
    }

    private PostDetails post(String postId) {
        return PostDetails.builder().postId(postId).build();
    }
}
