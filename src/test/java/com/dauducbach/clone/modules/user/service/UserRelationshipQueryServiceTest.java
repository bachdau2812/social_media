package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.UserFollower;
import com.dauducbach.clone.modules.user.repository.UserFollowerRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserRelationshipQueryServiceTest {
    @Mock
    UserFollowerRepository userFollowerRepository;

    @Test
    void followerPaginationClampsPageSizeAndKeepsResponseShape() {
        UserRelationshipQueryService service = new UserRelationshipQueryService(userFollowerRepository);
        UserFollower relation = UserFollower.builder()
                .id("follow-1")
                .followerId("follower-1")
                .followingId("owner-1")
                .createdAt(Instant.parse("2026-10-01T00:00:00Z"))
                .build();
        when(userFollowerRepository.countFollowers("owner-1")).thenReturn(Mono.just(250L));
        when(userFollowerRepository.findFollowersByUserId("owner-1", 100, 100))
                .thenReturn(Flux.just(relation));

        StepVerifier.create(service.getFollowers("owner-1", 1, 1000))
                .assertNext(page -> {
                    assertThat(page.getFollowers()).hasSize(1);
                    assertThat(page.getFollowers().getFirst().getUserId()).isEqualTo("follower-1");
                    assertThat(page.getTotalCount()).isEqualTo(250);
                    assertThat(page.getCurrentPage()).isEqualTo(1);
                    assertThat(page.getPageSize()).isEqualTo(100);
                    assertThat(page.isHasNextPage()).isTrue();
                    assertThat(page.isHasPreviousPage()).isTrue();
                })
                .verifyComplete();

        verify(userFollowerRepository).findFollowersByUserId("owner-1", 100, 100);
    }

    @Test
    void feedBroadcastPagesLargeFollowerSetsAndRemovesDuplicateIds() {
        UserRelationshipQueryService service = new UserRelationshipQueryService(userFollowerRepository);
        when(userFollowerRepository.countFollowers("owner-1")).thenReturn(Mono.just(501L));
        when(userFollowerRepository.findFollowerIdsByUserId("owner-1", 500, 0))
                .thenReturn(Flux.range(0, 500).map(index -> "follower-" + index));
        when(userFollowerRepository.findFollowerIdsByUserId("owner-1", 500, 500))
                .thenReturn(Flux.just("follower-499", "follower-500"));

        StepVerifier.create(service.getFollowerIdsForFeedBroadcast("owner-1").collectList())
                .assertNext(ids -> {
                    assertThat(ids).hasSize(501);
                    assertThat(ids.getFirst()).isEqualTo("follower-0");
                    assertThat(ids.getLast()).isEqualTo("follower-500");
                })
                .verifyComplete();
    }

    @Test
    void profileSummaryUsesRelationshipOwnerAndViewerDirections() {
        UserRelationshipQueryService service = new UserRelationshipQueryService(userFollowerRepository);
        when(userFollowerRepository.countFollowers("target-1")).thenReturn(Mono.just(12L));
        when(userFollowerRepository.countFollowing("target-1")).thenReturn(Mono.just(7L));
        when(userFollowerRepository.countFriends("target-1")).thenReturn(Mono.just(3L));
        when(userFollowerRepository.existsByFollowerIdAndFollowingId("viewer-1", "target-1"))
                .thenReturn(Mono.just(true));
        when(userFollowerRepository.existsByFollowerIdAndFollowingId("target-1", "viewer-1"))
                .thenReturn(Mono.just(false));

        StepVerifier.create(service.getRelationshipSummary("viewer-1", "target-1"))
                .expectNext(new com.dauducbach.clone.modules.user.publicapi.UserRelationshipQuery.RelationshipSummary(
                        12, 7, 3, true, false))
                .verifyComplete();
    }
}
