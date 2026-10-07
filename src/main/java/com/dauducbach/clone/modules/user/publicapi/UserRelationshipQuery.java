package com.dauducbach.clone.modules.user.publicapi;

import com.dauducbach.clone.modules.user.dto.response.FollowResponse;
import com.dauducbach.clone.modules.user.dto.response.FollowerCountResponse;
import com.dauducbach.clone.modules.user.dto.response.FollowerListResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserRelationshipQuery {
    Mono<FollowResponse> getUserFollowerById(String id);

    Mono<FollowerListResponse> getFollowers(String userId, int page, int size);

    Mono<FollowerListResponse> getFollowing(String userId, int page, int size);

    Mono<Boolean> isFollowing(String followerId, String followingId);

    Flux<String> getFollowerIdsForFeedBroadcast(String userId);

    Mono<FollowerCountResponse> getFollowerCounts(String userId);

    Mono<RelationshipSummary> getRelationshipSummary(String viewerId, String targetId);

    Flux<ConnectionSnapshot> getConnections(String userId, String tab, int limit);

    record RelationshipSummary(
            long followerCount,
            long followingCount,
            long friendCount,
            boolean viewerFollowsUser,
            boolean userFollowsViewer) {
    }

    record ConnectionSnapshot(String id, String followerId, String followingId, java.time.Instant createdAt) {
    }
}
