package com.dauducbach.clone.modules.user.relationship.application;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.user.dto.request.FollowRequest;
import com.dauducbach.clone.modules.user.entity.UserFollower;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FollowUseCaseTest {
    @Mock FollowRelationshipStore relationshipStore;
    @Mock FollowEventPublisher eventPublisher;

    @Test
    void selfFollowIsRejectedBeforePersistenceOrEventPublication() {
        FollowUseCase useCase = new FollowUseCase(relationshipStore, eventPublisher);

        StepVerifier.create(useCase.followUser(new FollowRequest("user-1", "user-1")))
                .expectError(AppException.class)
                .verify();

        verifyNoInteractions(relationshipStore, eventPublisher);
    }

    @Test
    void duplicateFollowIsRejectedWithoutAnotherInsertOrEvent() {
        FollowUseCase useCase = new FollowUseCase(relationshipStore, eventPublisher);
        when(relationshipStore.exists("follower-1", "owner-1")).thenReturn(Mono.just(true));

        StepVerifier.create(useCase.followUser(new FollowRequest("follower-1", "owner-1")))
                .expectError(AppException.class)
                .verify();

        verifyNoInteractions(eventPublisher);
    }

    @Test
    void followPersistsBeforePublishingAndKeepsResponseFields() {
        FollowUseCase useCase = new FollowUseCase(relationshipStore, eventPublisher);
        UserFollower saved = UserFollower.builder()
                .id("follow-1")
                .followerId("follower-1")
                .followingId("owner-1")
                .createdAt(Instant.parse("2026-10-01T00:00:00Z"))
                .build();
        when(relationshipStore.exists("follower-1", "owner-1")).thenReturn(Mono.just(false));
        when(relationshipStore.insert(any(UserFollower.class))).thenReturn(Mono.just(saved));
        when(eventPublisher.followed("follower-1", "owner-1")).thenReturn(Mono.empty());

        StepVerifier.create(useCase.followUser(new FollowRequest("follower-1", "owner-1")))
                .assertNext(response -> {
                    assertThat(response.getId()).isEqualTo("follow-1");
                    assertThat(response.getFollowerId()).isEqualTo("follower-1");
                    assertThat(response.getFollowingId()).isEqualTo("owner-1");
                    assertThat(response.getMessage()).isEqualTo("Successfully followed user");
                })
                .verifyComplete();

        var order = inOrder(relationshipStore, eventPublisher);
        order.verify(relationshipStore).insert(any(UserFollower.class));
        order.verify(eventPublisher).followed("follower-1", "owner-1");
    }

    @Test
    void unfollowDeletesBeforePublishing() {
        FollowUseCase useCase = new FollowUseCase(relationshipStore, eventPublisher);
        when(relationshipStore.exists("follower-1", "owner-1")).thenReturn(Mono.just(true));
        when(relationshipStore.delete("follower-1", "owner-1")).thenReturn(Mono.empty());
        when(eventPublisher.unfollowed("follower-1", "owner-1")).thenReturn(Mono.empty());

        StepVerifier.create(useCase.unfollowUser("follower-1", "owner-1"))
                .expectNext("Successfully unfollowed user")
                .verifyComplete();

        var order = inOrder(relationshipStore, eventPublisher);
        order.verify(relationshipStore).delete("follower-1", "owner-1");
        order.verify(eventPublisher).unfollowed("follower-1", "owner-1");
    }
}
