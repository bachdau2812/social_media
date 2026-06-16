package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.request.FollowRequest;
import com.dauducbach.clone.modules.user.dto.response.FollowResponse;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.entity.UserFollower;
import com.dauducbach.clone.modules.user.repositoty.UserFollowerRepository;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class UserFollowerService {
    UserFollowerRepository userFollowerRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    KafkaSender<String, String> kafkaSender;

    private static final Logger log = LoggerFactory.getLogger(UserFollowerService.class);
    private static final int DEFAULT_PAGE_SIZE = 20;

    /// 1. Theo dõi user (Follow) - Insert vào DB
    public Mono<FollowResponse> followUser(FollowRequest request) {
        log.info("|UserFollowerService|followUser|followerId={}|followingId={}", request.getFollowerId(), request.getFollowingId());

        // Validate: Không thể tự follow mình
        if (request.getFollowerId().equals(request.getFollowingId())) {
            return Mono.error(new AppException(
                    ErrorCode.CANNOT_FOLLOW_SELF,
                    String.format("Cannot follow yourself: followerId=%s, followingId=%s", request.getFollowerId(), request.getFollowingId())
            ));
        }

        JsonObject payload = new JsonObject();
        payload.addProperty("followerId", request.getFollowerId());
        payload.addProperty("followingId", request.getFollowingId());

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("follow_event", request.getFollowerId(), payload.toString());
        SenderRecord<String, String, String> senderRecord = SenderRecord.create(producerRecord, "Follow Event");

        // Check nếu đã follow rồi thì báo lỗi
        return userFollowerRepository.existsByFollowerIdAndFollowingId(request.getFollowerId(), request.getFollowingId())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new AppException(
                                ErrorCode.ALREADY_FOLLOWING_USER,
                                String.format("Already following this user: followerId=%s, followingId=%s", request.getFollowerId(), request.getFollowingId())
                        ));
                    }

                    // Insert vào DB
                    UserFollower userFollower = UserFollower.create(request.getFollowerId(), request.getFollowingId());
                    return r2dbcEntityTemplate.insert(UserFollower.class)
                            .using(userFollower)
                            .flatMap(userFollower1 -> kafkaSender.send(Mono.just(senderRecord))
                                    .doOnError(error -> log.error("|UserFollowerService|followUser|failed to send Kafka message|error={}", error.getMessage()))
                                    .then(Mono.just(userFollower1))
                            )
                            .map(saved -> FollowResponse.builder()
                                    .id(saved.getId())
                                    .followerId(saved.getFollowerId())
                                    .followingId(saved.getFollowingId())
                                    .createdAt(saved.getCreatedAt())
                                    .message("Successfully followed user")
                                    .build())
                            .onErrorMap(throwable -> throwable instanceof AppException
                                    ? throwable
                                    : new AppException(
                                            ErrorCode.FOLLOW_SAVE_FAILED,
                                            String.format("Follow user failed: followerId=%s, followingId=%s", request.getFollowerId(), request.getFollowingId()),
                                            throwable
                                    ))
                            .doOnSuccess(response -> log.info("|UserFollowerService|followUser|created|id={}", response.getId()))
                            .doOnError(error -> log.error("|UserFollowerService|followUser|failed to follow|error={}", error.getMessage()));
                });
    }

    /// 2. Bỏ theo dõi (Unfollow) - Xóa khỏi DB
    public Mono<String> unfollowUser(String followerId, String followingId) {
        log.info("|UserFollowerService|unfollowUser|followerId={}|followingId={}", followerId, followingId);

        // Build event to send to analysis-service
        JsonObject payload = new JsonObject();
        payload.addProperty("followerId", followerId);
        payload.addProperty("followingId", followingId);

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("un_follow_event", followerId, payload.toString());
        SenderRecord<String, String, String> senderRecord = SenderRecord.create(producerRecord, "Unfollow Event");

        // Check relationship
        return userFollowerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new AppException(
                                ErrorCode.NOT_FOLLOWING_USER,
                                String.format("Not following this user: followerId=%s, followingId=%s", followerId, followingId)
                        ));
                    }

                    // Delete vào DB
                    return userFollowerRepository.deleteByFollowerIdAndFollowingId(followerId, followingId)
                            .then(kafkaSender.send(Mono.just(senderRecord))
                                    .doOnComplete(() -> log.info("|UserFollowerService|unfollowUser|sent Kafka message for unfollow event|followerId={}|followingId={}", followerId, followingId))
                                    .doOnError(error -> log.error("|UserFollowerService|unfollowUser|failed to send Kafka message for unfollow event|error={}", error.getMessage()))
                                    .then())
                            .then(Mono.just("Successfully unfollowed user"))
                            .onErrorMap(throwable -> throwable instanceof AppException
                                    ? throwable
                                    : new AppException(
                                            ErrorCode.UNFOLLOW_FAILED,
                                            String.format("Unfollow user failed: followerId=%s, followingId=%s", followerId, followingId),
                                            throwable
                                    ))
                            .doOnSuccess(message -> log.info("|UserFollowerService|unfollowUser|unfollowed|followerId={}|followingId={}", followerId, followingId))
                            .doOnError(error -> log.error("|UserFollowerService|unfollowUser|failed to unfollow|error={}", error.getMessage()));
                });
    }

    /// 3. Lấy UserFollower theo ID
    public Mono<FollowResponse> getUserFollowerById(String id) {
        log.info("|UserFollowerService|getUserFollowerById|id={}", id);

        return userFollowerRepository.findById(id)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.FOLLOW_RELATIONSHIP_NOT_FOUND,
                        String.format("Follow relationship not found for id=%s", id)
                )))
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.FOLLOW_RELATIONSHIP_FETCH_FAILED,
                                String.format("Fetch follow relationship failed for id=%s", id),
                                throwable
                        ))
                .map(follower -> FollowResponse.builder()
                        .id(follower.getId())
                        .followerId(follower.getFollowerId())
                        .followingId(follower.getFollowingId())
                        .createdAt(follower.getCreatedAt())
                        .message("Follow relationship found")
                        .build())
                .doOnSuccess(response -> log.info("|UserFollowerService|getUserFollowerById|found|id={}", id))
                .doOnError(error -> log.error("|UserFollowerService|getUserFollowerById|not found|id={}", id));
    }

    /// 4. Lấy danh sách những người đang follow một người (Followers) với phân trang
    public Mono<FollowerListResponse> getFollowers(String userId, int page, int size) {
        log.info("|UserFollowerService|getFollowers|userId={}|page={}|size={}", userId, page, size);

        int validatedSize = validatePageSize(size);
        int offset = page * validatedSize;

        // Get total count
        return userFollowerRepository.countFollowers(userId)
                .flatMap(totalCount -> {
                    // Get followers với pagination
                    return userFollowerRepository.findFollowersByUserId(userId, validatedSize, offset)
                            .onErrorMap(throwable -> new AppException(
                                    ErrorCode.FOLLOWERS_FETCH_FAILED,
                                    String.format("Fetch followers failed for userId=%s", userId),
                                    throwable
                            ))
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
                                    .build())
                            .doOnSuccess(response -> log.info("|UserFollowerService|getFollowers|found {} followers for userId={}", response.getFollowers().size(), userId));
                })
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.FOLLOWERS_FETCH_FAILED,
                                String.format("Fetch followers failed for userId=%s", userId),
                                throwable
                        ))
                .doOnError(error -> log.error("|UserFollowerService|getFollowers|failed|userId={}|error={}", userId, error.getMessage()));
    }

    /// 5. Lấy danh sách những người mà một user đang following (Following) với phân trang
    public Mono<FollowerListResponse> getFollowing(String userId, int page, int size) {
        log.info("|UserFollowerService|getFollowing|userId={}|page={}|size={}", userId, page, size);

        int validatedSize = validatePageSize(size);
        int offset = page * validatedSize;

        // Get total count
        return userFollowerRepository.countFollowing(userId)
                .flatMap(totalCount -> {
                    // Get following với pagination
                    return userFollowerRepository.findFollowingByUserId(userId, validatedSize, offset)
                            .onErrorMap(throwable -> new AppException(
                                    ErrorCode.FOLLOWING_FETCH_FAILED,
                                    String.format("Fetch following failed for userId=%s", userId),
                                    throwable
                            ))
                            .map(following -> FollowerListResponse.FollowerInfo.builder()
                                    .followId(following.getId())
                                    .userId(following.getFollowingId())
                                    .followedAt(following.getCreatedAt().toString())
                                    .build())
                            .collectList()
                            .map(followingInfos -> FollowerListResponse.builder()
                                    .followers(followingInfos) // Reuse same structure - nghĩa đây là "following"
                                    .totalCount(totalCount.intValue())
                                    .currentPage(page)
                                    .pageSize(validatedSize)
                                    .hasNextPage((page + 1) * validatedSize < totalCount)
                                    .hasPreviousPage(page > 0)
                                    .build())
                            .doOnSuccess(response -> log.info("|UserFollowerService|getFollowing|found {} following for userId={}", response.getFollowers().size(), userId));
                })
                .onErrorMap(throwable -> throwable instanceof AppException
                        ? throwable
                        : new AppException(
                                ErrorCode.FOLLOWING_FETCH_FAILED,
                                String.format("Fetch following failed for userId=%s", userId),
                                throwable
                        ))
                .doOnError(error -> log.error("|UserFollowerService|getFollowing|failed|userId={}|error={}", userId, error.getMessage()));
    }

    /// Check nếu user A đang follow user B
    public Mono<Boolean> isFollowing(String followerId, String followingId) {
        log.info("|UserFollowerService|isFollowing|followerId={}|followingId={}", followerId, followingId);

        return userFollowerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .onErrorMap(throwable -> new AppException(
                        ErrorCode.FOLLOW_STATUS_CHECK_FAILED,
                        String.format("Check following status failed: followerId=%s, followingId=%s", followerId, followingId),
                        throwable
                ))
                .doOnSuccess(isFollowing -> log.info("|UserFollowerService|isFollowing|result={}|followerId={}|followingId={}", isFollowing, followerId, followingId));
    }

    /// Get follower counts
    public Mono<FollowerCountResponse> getFollowerCounts(String userId) {
        log.info("|UserFollowerService|getFollowerCounts|userId={}", userId);

        return Mono.zip(
                userFollowerRepository.countFollowers(userId),
                userFollowerRepository.countFollowing(userId)
        )
        .onErrorMap(throwable -> new AppException(
                ErrorCode.FOLLOW_COUNT_FAILED,
                String.format("Fetch follower counts failed for userId=%s", userId),
                throwable
        ))
        .map(tuple -> FollowerCountResponse.builder()
                .userId(userId)
                .followersCount(tuple.getT1().intValue())
                .followingCount(tuple.getT2().intValue())
                .build())
        .doOnSuccess(counts -> log.info("|UserFollowerService|getFollowerCounts|userId={}|followers={}|following={}", userId, counts.getFollowersCount(), counts.getFollowingCount()));
    }

    /// Helper: Validate page size
    private int validatePageSize(int size) {
        if (size <= 0) return DEFAULT_PAGE_SIZE;
        if (size > 100) return 100; // Max 100 items per page
        return size;
    }

    /// Response DTO cho follower counts
    @lombok.Data
    @lombok.NoArgsConstructor
    @lombok.AllArgsConstructor
    @lombok.experimental.FieldDefaults(level = lombok.AccessLevel.PRIVATE)
    @lombok.Builder
    public static class FollowerCountResponse {
        String userId;
        int followersCount;    // Số người đang follow user này
        int followingCount;     // Số người user này đang follow
    }
}
