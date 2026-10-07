package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.post.publicapi.PostFeedQuery;
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
        FeedVectorSnapshotService vectorService = mock(FeedVectorSnapshotService.class);
        PostFeedQuery postQuery = mock(PostFeedQuery.class);
        FeedCandidatePipeline pipeline = new FeedCandidatePipeline(vectorService, postQuery);

        when(vectorService.load("user-1")).thenReturn(Mono.just(new com.dauducbach.clone.modules.feed.dto.FeedVectorSnapshot(1L, List.of(0.2, 0.8))));
        when(postQuery.searchRecommendedPostIds(List.of(0.2, 0.8), 10, Set.of("seen")))
                .thenReturn(Mono.just(List.of("vector-1", "duplicate")));
        when(postQuery.findRecentApprovedPostIds(10, Set.of("seen")))
                .thenReturn(Flux.just("duplicate", "recent-1"));

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
    void recentSourceRemainsAvailableWhenVectorSnapshotFails() {
        FeedVectorSnapshotService vectorService = mock(FeedVectorSnapshotService.class);
        PostFeedQuery postQuery = mock(PostFeedQuery.class);
        FeedCandidatePipeline pipeline = new FeedCandidatePipeline(vectorService, postQuery);

        when(vectorService.load("user-1")).thenReturn(Mono.error(new IllegalStateException("vector unavailable")));
        when(postQuery.findRecentApprovedPostIds(5, Set.of())).thenReturn(Flux.just("recent-1"));

        StepVerifier.create(pipeline.select("user-1", 5, Set.of()))
                .assertNext(candidates -> {
                    assertThat(candidates).hasSize(1);
                    assertThat(candidates.getFirst().sourceType()).isEqualTo("recent");
                })
                .verifyComplete();
    }

}
