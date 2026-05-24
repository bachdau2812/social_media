package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserFollower;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface UserFollowerRepository extends ReactiveCrudRepository<UserFollower, String> {

    Mono<Boolean> existsByFollowerIdAndFollowingId(String followerId, String followingId);

    // Get all followers of a user
    @Query("SELECT * FROM user_follower WHERE following_id = :userId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<UserFollower> findFollowersByUserId(String userId, int limit, int offset);

    // Get all users that a user is following
    @Query("SELECT * FROM user_follower WHERE follower_id = :userId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<UserFollower> findFollowingByUserId(String userId, int limit, int offset);

    // Count total followers (Dùng @Query ở đây là OK)
    @Query("SELECT COUNT(*) FROM user_follower WHERE following_id = :userId")
    Mono<Long> countFollowers(String userId);

    // Count total following (Dùng @Query ở đây là OK)
    @Query("SELECT COUNT(*) FROM user_follower WHERE follower_id = :userId")
    Mono<Long> countFollowing(String userId);

    Mono<Void> deleteByFollowerIdAndFollowingId(String followerId, String followingId);
}