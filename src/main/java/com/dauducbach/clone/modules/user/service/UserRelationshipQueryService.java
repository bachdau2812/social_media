package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.response.FollowResponse;
import com.dauducbach.clone.modules.user.dto.response.FollowerCountResponse;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.entity.UserFollower;
import com.dauducbach.clone.modules.user.publicapi.UserRelationshipQuery;
import com.dauducbach.clone.modules.user.repository.UserFollowerRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Locale;

@Service
@RequiredArgsConstructor
public class UserRelationshipQueryService implements UserRelationshipQuery {
    private static final Logger log = LoggerFactory.getLogger(UserRelationshipQueryService.class);
    private static final int DEFAULT_PAGE_SIZE = 20;
    private static final int FEED_BROADCAST_PAGE_SIZE = 500;

    private final UserFollowerRepository userFollowerRepository;

    @Override
    public Mono<FollowResponse> getUserFollowerById(String id) {
        return userFollowerRepository.findById(id)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.FOLLOW_RELATIONSHIP_NOT_FOUND,
                        String.format("Follow relationship not found for id=%s", id))))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.FOLLOW_RELATIONSHIP_FETCH_FAILED,
                                String.format("Fetch follow relationship failed for id=%s", id),
                                throwable))
                .map(follower -> FollowResponse.builder()
                        .id(follower.getId())
                        .followerId(follower.getFollowerId())
                        .followingId(follower.getFollowingId())
                        .createdAt(follower.getCreatedAt())
                        .message("Follow relationship found")
                        .build());
    }

    @Override
    public Mono<FollowerListResponse> getFollowers(String userId, int page, int size) {
        int validatedSize = validatePageSize(size);
        int offset = page * validatedSize;
        return userFollowerRepository.countFollowers(userId)
                .flatMap(totalCount -> userFollowerRepository
                        .findFollowersByUserId(userId, validatedSize, offset)
                        .onErrorMap(throwable -> new AppException(
                                ErrorCode.FOLLOWERS_FETCH_FAILED,
                                String.format("Fetch followers failed for userId=%s", userId),
                                throwable))
                        .map(follower -> FollowerListResponse.FollowerInfo.builder()
                                .followId(follower.getId())
                                .userId(follower.getFollowerId())
                                .followedAt(follower.getCreatedAt().toString())
                                .build())
                        .collectList()
                        .map(followerInfos -> FollowerListResponse.builder()
                                .followers(followerInfos)
                                .totalCount(totalCount.intValue())
                                .currentPage(page)
                                .pageSize(validatedSize)
                                .hasNextPage((page + 1) * validatedSize < totalCount)
                                .hasPreviousPage(page > 0)
                                .build()))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.FOLLOWERS_FETCH_FAILED,
                                String.format("Fetch followers failed for userId=%s", userId),
                                throwable));
    }

    @Override
    public Mono<FollowerListResponse> getFollowing(String userId, int page, int size) {
        int validatedSize = validatePageSize(size);
        int offset = page * validatedSize;
        return userFollowerRepository.countFollowing(userId)
                .flatMap(totalCount -> userFollowerRepository
                        .findFollowingByUserId(userId, validatedSize, offset)
                        .onErrorMap(throwable -> new AppException(
                                ErrorCode.FOLLOWING_FETCH_FAILED,
                                String.format("Fetch following failed for userId=%s", userId),
                                throwable))
                        .map(following -> FollowerListResponse.FollowerInfo.builder()
                                .followId(following.getId())
                                .userId(following.getFollowingId())
                                .followedAt(following.getCreatedAt().toString())
                                .build())
                        .collectList()
                        .map(followingInfos -> FollowerListResponse.builder()
                                .followers(followingInfos)
                                .totalCount(totalCount.intValue())
                                .currentPage(page)
                                .pageSize(validatedSize)
                                .hasNextPage((page + 1) * validatedSize < totalCount)
                                .hasPreviousPage(page > 0)
                                .build()))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.FOLLOWING_FETCH_FAILED,
                                String.format("Fetch following failed for userId=%s", userId),
                                throwable));
    }

    @Override
    public Mono<Boolean> isFollowing(String followerId, String followingId) {
        return userFollowerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.FOLLOW_STATUS_CHECK_FAILED,
                        String.format("Check following status failed: followerId=%s, followingId=%s", followerId, followingId),
                        throwable));
    }

    @Override
    public Flux<String> findFollowingIds(String followerId, java.util.Collection<String> candidateIds) {
        var ids = candidateIds == null ? java.util.List.<String>of() : candidateIds.stream()
                .filter(id -> id != null && !id.isBlank()).distinct().toList();
        return ids.isEmpty() ? Flux.empty() : userFollowerRepository.findFollowingIds(followerId, ids);
    }

    @Override
    public Flux<String> getFollowerIdsForFeedBroadcast(String userId) {
        if (userId == null || userId.isBlank()) {
            return Flux.empty();
        }
        return userFollowerRepository.countFollowers(userId)
                .flatMapMany(total -> {
                    int pageCount = (int) Math.ceil((double) total / FEED_BROADCAST_PAGE_SIZE);
                    if (pageCount <= 0) {
                        return Flux.empty();
                    }
                    return Flux.range(0, pageCount)
                            .concatMap(page -> userFollowerRepository.findFollowerIdsByUserId(
                                    userId,
                                    FEED_BROADCAST_PAGE_SIZE,
                                    page * FEED_BROADCAST_PAGE_SIZE));
                })
                .filter(followerId -> followerId != null && !followerId.isBlank())
                .distinct()
                .onErrorResume(error -> {
                    log.error("|UserRelationshipQueryService|getFollowerIdsForFeedBroadcast|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Flux.empty();
                });
    }

    @Override
    public Mono<FollowerCountResponse> getFollowerCounts(String userId) {
        return Mono.zip(
                        userFollowerRepository.countFollowers(userId),
                        userFollowerRepository.countFollowing(userId))
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.FOLLOW_COUNT_FAILED,
                        String.format("Fetch follower counts failed for userId=%s", userId),
                        throwable))
                .map(tuple -> new FollowerCountResponse(userId, tuple.getT1().intValue(), tuple.getT2().intValue()));
    }

    @Override
    public Mono<RelationshipSummary> getRelationshipSummary(String viewerId, String targetId) {
        return Mono.zip(
                userFollowerRepository.countFollowers(targetId).defaultIfEmpty(0L),
                userFollowerRepository.countFollowing(targetId).defaultIfEmpty(0L),
                userFollowerRepository.countFriends(targetId).defaultIfEmpty(0L),
                userFollowerRepository.existsByFollowerIdAndFollowingId(viewerId, targetId).defaultIfEmpty(false),
                userFollowerRepository.existsByFollowerIdAndFollowingId(targetId, viewerId).defaultIfEmpty(false))
                .map(tuple -> new RelationshipSummary(
                        tuple.getT1(), tuple.getT2(), tuple.getT3(),
                        Boolean.TRUE.equals(tuple.getT4()), Boolean.TRUE.equals(tuple.getT5())));
    }

    @Override
    public Flux<ConnectionSnapshot> getConnections(String userId, String tab, int limit) {
        String normalizedTab = tab == null ? "FOLLOWERS" : tab.trim().toUpperCase(Locale.ROOT);
        Flux<UserFollower> source = switch (normalizedTab) {
            case "FOLLOWING" -> userFollowerRepository.findFollowingByUserId(userId, limit, 0);
            case "FRIENDS" -> userFollowerRepository.findFriendsByUserId(userId, limit, 0);
            default -> userFollowerRepository.findFollowersByUserId(userId, limit, 0);
        };
        return source.map(row -> new ConnectionSnapshot(
                row.getId(), row.getFollowerId(), row.getFollowingId(), row.getCreatedAt()));
    }

    private int validatePageSize(int size) {
        if (size <= 0) return DEFAULT_PAGE_SIZE;
        return Math.min(size, 100);
    }
}
