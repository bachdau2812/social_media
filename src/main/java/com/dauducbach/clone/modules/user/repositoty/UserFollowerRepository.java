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

    @Query("SELECT follower_id FROM user_follower WHERE following_id = :userId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<String> findFollowerIdsByUserId(String userId, int limit, int offset);

    // Get all users that a user is following
    @Query("SELECT * FROM user_follower WHERE follower_id = :userId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<UserFollower> findFollowingByUserId(String userId, int limit, int offset);

    @Query("""
            SELECT following.*
            FROM user_follower following
            INNER JOIN user_follower follower_back
              ON follower_back.follower_id = following.following_id
             AND follower_back.following_id = :userId
            WHERE following.follower_id = :userId
            ORDER BY following.created_at DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<UserFollower> findFriendsByUserId(String userId, int limit, int offset);

    // Count total followers
    @Query("SELECT COUNT(*) FROM user_follower WHERE following_id = :userId")
    Mono<Long> countFollowers(String userId);

    // Count total following
    @Query("SELECT COUNT(*) FROM user_follower WHERE follower_id = :userId")
    Mono<Long> countFollowing(String userId);

    @Query("""
            SELECT COUNT(*)
            FROM user_follower following
            INNER JOIN user_follower follower_back
              ON follower_back.follower_id = following.following_id
             AND follower_back.following_id = :userId
            WHERE following.follower_id = :userId
            """)
    Mono<Long> countFriends(String userId);

    Mono<Void> deleteByFollowerIdAndFollowingId(String followerId, String followingId);
}