package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.FollowRequest;
import com.dauducbach.clone.modules.user.dto.response.FollowResponse;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import com.dauducbach.clone.modules.user.service.UserFollowerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-followers")
public class UserFollowerController {
    private final UserFollowerService userFollowerService;

    /// 1. Theo dõi user (Follow)
    @PostMapping("/follow")
    public Mono<ApiResponse<FollowResponse>> followUser(@Valid @RequestBody FollowRequest request) {
        return userFollowerService.followUser(request)
                .map(response -> ApiResponse.<FollowResponse>builder()
                        .message(response.getMessage())
                        .result(response)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<FollowResponse>builder()
                        .message("Error following user: " + error.getMessage())
                        .build()));
    }

    /// 2. Bỏ theo dõi (Unfollow)
    @DeleteMapping("/unfollow")
    public Mono<ApiResponse<String>> unfollowUser(
            @RequestParam String followerId,
            @RequestParam String followingId) {
        return userFollowerService.unfollowUser(followerId, followingId)
                .map(message -> ApiResponse.<String>builder()
                        .message(message)
                        .result(message)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error unfollowing user: " + error.getMessage())
                        .build()));
    }

    /// 3. Lấy UserFollower theo ID
    @GetMapping("/{id}")
    public Mono<ApiResponse<FollowResponse>> getUserFollowerById(@PathVariable String id) {
        return userFollowerService.getUserFollowerById(id)
                .map(response -> ApiResponse.<FollowResponse>builder()
                        .message(response.getMessage())
                        .result(response)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<FollowResponse>builder()
                        .message("Error retrieving follow relationship: " + error.getMessage())
                        .build()));
    }

    /// 4. Lấy danh sách những người đang follow một user (Followers) với phân trang
    @GetMapping("/followers/{userId}")
    public Mono<ApiResponse<FollowerListResponse>> getFollowers(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return userFollowerService.getFollowers(userId, page, size)
                .map(response -> ApiResponse.<FollowerListResponse>builder()
                        .message("Followers retrieved successfully")
                        .result(response)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<FollowerListResponse>builder()
                        .message("Error retrieving followers: " + error.getMessage())
                        .build()));
    }

    /// 5. Lấy danh sách những người mà một user đang following (Following) với phân trang
    @GetMapping("/following/{userId}")
    public Mono<ApiResponse<FollowerListResponse>> getFollowing(
            @PathVariable String userId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return userFollowerService.getFollowing(userId, page, size)
                .map(response -> ApiResponse.<FollowerListResponse>builder()
                        .message("Following retrieved successfully")
                        .result(response)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<FollowerListResponse>builder()
                        .message("Error retrieving following: " + error.getMessage())
                        .build()));
    }

    /// Check xem user A có đang follow user B không
    @GetMapping("/is-following")
    public Mono<ApiResponse<Boolean>> isFollowing(
            @RequestParam String followerId,
            @RequestParam String followingId) {
        return userFollowerService.isFollowing(followerId, followingId)
                .map(isFollowing -> ApiResponse.<Boolean>builder()
                        .message("Follow status checked successfully")
                        .result(isFollowing)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<Boolean>builder()
                        .message("Error checking follow status: " + error.getMessage())
                        .result(false)
                        .build()));
    }

    /// Lấy follower counts
    @GetMapping("/counts/{userId}")
    public Mono<ApiResponse<UserFollowerService.FollowerCountResponse>> getFollowerCounts(@PathVariable String userId) {
        return userFollowerService.getFollowerCounts(userId)
                .map(counts -> ApiResponse.<UserFollowerService.FollowerCountResponse>builder()
                        .message("Follower counts retrieved successfully")
                        .result(counts)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserFollowerService.FollowerCountResponse>builder()
                        .message("Error retrieving follower counts: " + error.getMessage())
                        .build()));
    }
}