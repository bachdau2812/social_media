package com.dauducbach.clone.modules.user.relationship.application;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.request.FollowRequest;
import com.dauducbach.clone.modules.user.dto.response.FollowResponse;
import com.dauducbach.clone.modules.user.entity.UserFollower;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class FollowUseCase {
    private static final Logger log = LoggerFactory.getLogger(FollowUseCase.class);

    private final FollowRelationshipStore relationshipStore;
    private final FollowEventPublisher eventPublisher;

    public FollowUseCase(FollowRelationshipStore relationshipStore, FollowEventPublisher eventPublisher) {
        this.relationshipStore = relationshipStore;
        this.eventPublisher = eventPublisher;
    }

    public Mono<FollowResponse> followUser(FollowRequest request) {
        String followerId = request.getFollowerId();
        String followingId = request.getFollowingId();
        if (followerId.equals(followingId)) {
            return Mono.error(new AppException(
                    ErrorCode.CANNOT_FOLLOW_SELF,
                    String.format("Cannot follow yourself: followerId=%s, followingId=%s", followerId, followingId)));
        }

        return relationshipStore.exists(followerId, followingId)
                .flatMap(exists -> {
                    if (Boolean.TRUE.equals(exists)) {
                        return Mono.error(new AppException(
                                ErrorCode.ALREADY_FOLLOWING_USER,
                                String.format("Already following this user: followerId=%s, followingId=%s", followerId, followingId)));
                    }

                    return relationshipStore.insert(UserFollower.create(followerId, followingId))
                            .flatMap(saved -> eventPublisher.followed(followerId, followingId).thenReturn(saved))
                            .map(saved -> FollowResponse.builder()
                                    .id(saved.getId())
                                    .followerId(saved.getFollowerId())
                                    .followingId(saved.getFollowingId())
                                    .createdAt(saved.getCreatedAt())
                                    .message("Successfully followed user")
                                    .build())
                            .onErrorMap(error -> error instanceof AppException
                                    ? error
                                    : new AppException(
                                            ErrorCode.FOLLOW_SAVE_FAILED,
                                            String.format("Follow user failed: followerId=%s, followingId=%s", followerId, followingId),
                                            error));
                })
                .doOnSuccess(response -> log.info("|FollowUseCase|follow|created|relationshipId={}", response.getId()))
                .doOnError(error -> log.error("|FollowUseCase|follow|failed|error={}", error.getMessage()));
    }

    public Mono<String> unfollowUser(String followerId, String followingId) {
        return relationshipStore.exists(followerId, followingId)
                .flatMap(exists -> {
                    if (!Boolean.TRUE.equals(exists)) {
                        return Mono.error(new AppException(
                                ErrorCode.NOT_FOLLOWING_USER,
                                String.format("Not following this user: followerId=%s, followingId=%s", followerId, followingId)));
                    }
                    return relationshipStore.delete(followerId, followingId)
                            .then(eventPublisher.unfollowed(followerId, followingId))
                            .thenReturn("Successfully unfollowed user")
                            .onErrorMap(error -> error instanceof AppException
                                    ? error
                                    : new AppException(
                                            ErrorCode.UNFOLLOW_FAILED,
                                            String.format("Unfollow user failed: followerId=%s, followingId=%s", followerId, followingId),
                                            error));
                })
                .doOnError(error -> log.error("|FollowUseCase|unfollow|failed|error={}", error.getMessage()));
    }
}
