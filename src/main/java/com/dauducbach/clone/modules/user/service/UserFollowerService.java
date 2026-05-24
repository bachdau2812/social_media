package com.dauducbach.clone.modules.user.service;

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
    KafkaSender<String, Object> kafkaSender;

    private static final Logger log = LoggerFactory.getLogger(UserFollowerService.class);
    private static final int DEFAULT_PAGE_SIZE = 20;

    /// 1. Theo dõi user (Follow) - Insert vào DB
    public Mono<FollowResponse> followUser(FollowRequest request) {
        log.info("|UserFollowerService|followUser|followerId={}|followingId={}", request.getFollowerId(), request.getFollowingId());

        // Validate: Không thể tự follow mình
        if (request.getFollowerId().equals(request.getFollowingId())) {
            return Mono.error(new RuntimeException("Cannot follow yourself"));
        }

        // Build record to send to analysis-service
        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("follow_event", request.getFollowerId(), request.toString());
        SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "Follow Event");

        // Check nếu đã follow rồi thì báo lỗi
        return userFollowerRepository.existsByFollowerIdAndFollowingId(request.getFollowerId(), request.getFollowingId())
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.error(new RuntimeException("Already following this user"));
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

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("un_follow_event", followerId, payload.toString());
        SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "Unfollow Event");

        // Check relationship
        return userFollowerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new RuntimeException("Not following this user"));
                    }

                    // Delete vào DB
                    return userFollowerRepository.deleteByFollowerIdAndFollowingId(followerId, followingId)
                            .then(kafkaSender.send(Mono.just(senderRecord))
                                    .doOnComplete(() -> log.info("|UserFollowerService|unfollowUser|sent Kafka message for unfollow event|followerId={}|followingId={}", followerId, followingId))
                                    .doOnError(error -> log.error("|UserFollowerService|unfollowUser|failed to send Kafka message for unfollow event|error={}", error.getMessage()))
                                    .then())
                            .then(Mono.just("Successfully unfollowed user"))
                            .doOnSuccess(message -> log.info("|UserFollowerService|unfollowUser|unfollowed|followerId={}|followingId={}", followerId, followingId))
                            .doOnError(error -> log.error("|UserFollowerService|unfollowUser|failed to unfollow|error={}", error.getMessage()));
                });
    }

    /// 3. Lấy UserFollower theo ID
    public Mono<FollowResponse> getUserFollowerById(String id) {
        log.info("|UserFollowerService|getUserFollowerById|id={}", id);

        return userFollowerRepository.findById(id)
                .switchIfEmpty(Mono.error(new RuntimeException("Follow relationship not found with id: " + id)))
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
                .doOnError(error -> log.error("|UserFollowerService|getFollowing|failed|userId={}|error={}", userId, error.getMessage()));
    }

    /// Check nếu user A đang follow user B
    public Mono<Boolean> isFollowing(String followerId, String followingId) {
        log.info("|UserFollowerService|isFollowing|followerId={}|followingId={}", followerId, followingId);

        return userFollowerRepository.existsByFollowerIdAndFollowingId(followerId, followingId)
                .doOnSuccess(isFollowing -> log.info("|UserFollowerService|isFollowing|result={}|followerId={}|followingId={}", isFollowing, followerId, followingId));
    }

    /// Get follower counts
    public Mono<FollowerCountResponse> getFollowerCounts(String userId) {
        log.info("|UserFollowerService|getFollowerCounts|userId={}", userId);

        return Mono.zip(
                userFollowerRepository.countFollowers(userId),
                userFollowerRepository.countFollowing(userId)
        )
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