package com.dauducbach.clone.modules.post.repository;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repository.projection.FriendFeedActivityProjection;
import com.dauducbach.clone.modules.post.repository.projection.PostInteractionSnapshotProjection;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface PostDetailsRepository extends ReactiveCrudRepository<PostDetails, String> {
    @Modifying
    @Query("""
            UPDATE post_details
            SET validate_status = :processing, updated_at = :updatedAt
            WHERE post_id = :postId AND validate_status = :pending
            """)
    Mono<Integer> claimPendingMediaScan(String postId, String pending, String processing,
            java.time.Instant updatedAt);

    @Modifying
    @Query("""
            UPDATE post_details
            SET validate_status = :pending, updated_at = :updatedAt
            WHERE post_id = :postId AND validate_status = :processing
            """)
    Mono<Integer> releaseMediaScanClaim(String postId, String pending, String processing,
            java.time.Instant updatedAt);

    @Query("""
            SELECT p.* FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND p.created_at <= :upperBound
              AND (:afterTime IS NULL OR p.created_at < :afterTime
                   OR (p.created_at = :afterTime AND p.post_id < :afterId))
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id AND UPPER(archived.content_type) = 'POST'
              )
              AND p.user_id IN (
                SELECT following.following_id FROM user_follower following
                INNER JOIN user_follower follower_back
                  ON follower_back.follower_id = following.following_id
                 AND follower_back.following_id = :userId
                WHERE following.follower_id = :userId
              )
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT :limit
            """)
    Flux<PostDetails> findApprovedFriendPostsBefore(String userId, java.time.Instant upperBound,
            java.time.Instant afterTime, String afterId, int limit);

    @Query("SELECT user_id FROM post_details WHERE post_id = :postId")
    Mono<String> findUserIdByPostId(String postId);

    @Query("""
            SELECT p.* FROM post_details p
            WHERE p.user_id = :userId
              AND p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<PostDetails> findByUserId(String userId, int limit, int offset);

    @Query("""
            SELECT COUNT(*) FROM post_details p
            WHERE p.user_id = :userId AND p.validate_status = 'APPROVED'
              AND NOT EXISTS (SELECT 1 FROM user_archive_items a WHERE a.content_id = p.post_id AND UPPER(a.content_type) = 'POST')
              AND (p.created_at > :createdAt OR (p.created_at = :createdAt AND p.post_id > :postId))
            """)
    Mono<Long> countEligibleAuthorPostsBefore(String userId, java.time.Instant createdAt, String postId);

    Flux<PostDetails> findAllByUserId(String userId);

    @Query("""
            SELECT p.* FROM post_details p
            WHERE p.post_id = :postId
              AND p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
            LIMIT 1
            """)
    Mono<PostDetails> findApprovedFeedEligibleById(String postId);

    @Query("""
            SELECT p.* FROM post_details p
            WHERE p.post_id IN (:postIds)
              AND p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
            """)
    Flux<PostDetails> findApprovedFeedEligibleByIdIn(java.util.Collection<String> postIds);

    @Query("""
            SELECT p.post_id AS post_id,
                   (SELECT COUNT(*) FROM likes l WHERE l.target_id = p.post_id AND l.target_type = 'POST') AS likes,
                   (SELECT COUNT(*) FROM comments c WHERE c.post_id = p.post_id) AS comments,
                   (SELECT COUNT(*) FROM post_reposts r WHERE r.post_id = p.post_id) AS reposts,
                   EXISTS(SELECT 1 FROM likes l WHERE l.target_id = p.post_id
                          AND l.target_type = 'POST' AND l.actor_id = :viewerId) AS liked_by_viewer,
                   EXISTS(SELECT 1 FROM post_reposts r WHERE r.post_id = p.post_id
                          AND r.actor_id = :viewerId) AS reposted_by_viewer
            FROM post_details p
            WHERE p.post_id IN (:postIds)
            """)
    Flux<PostInteractionSnapshotProjection> findInteractionSnapshots(
            java.util.Collection<String> postIds, String viewerId);

    @Query("""
            SELECT p.post_id FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
              AND (
                p.content LIKE :queryPattern
                OR p.hashtag LIKE :queryPattern
              )
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<String> searchApprovedPostIds(String queryPattern, Pageable pageable);

    @Query("""
            SELECT p.* FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT :limit
            """)
    Flux<PostDetails> findRecentApprovedPosts(int limit);

    @Query("""
            SELECT p.* FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
              AND p.user_id IN (
                SELECT following.following_id
                FROM user_follower following
                INNER JOIN user_follower follower_back
                  ON follower_back.follower_id = following.following_id
                 AND follower_back.following_id = :userId
                WHERE following.follower_id = :userId
              )
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT :limit
            OFFSET :offset
            """)
    Flux<PostDetails> findRecentApprovedPostsFromMutualFriends(String userId, int limit, int offset);

    @Query("""
            SELECT activity.feed_entry_id,
                   activity.post_id,
                   activity.activity_type,
                   activity.actor_id,
                   activity.activity_at
            FROM (
                SELECT CONCAT('post:', post.post_id) AS feed_entry_id,
                       post.post_id AS post_id,
                       'ORIGINAL_POST' AS activity_type,
                       post.user_id AS actor_id,
                       post.created_at AS activity_at
                FROM post_details post
                WHERE post.validate_status = 'APPROVED'
                  AND post.user_id IN (
                    SELECT following.following_id
                    FROM user_follower following
                    INNER JOIN user_follower follower_back
                      ON follower_back.follower_id = following.following_id
                     AND follower_back.following_id = :userId
                    WHERE following.follower_id = :userId
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM user_archive_items archived
                    WHERE archived.content_id = post.post_id
                      AND UPPER(archived.content_type) = 'POST'
                  )
                UNION ALL
                SELECT repost.id AS feed_entry_id,
                       repost.post_id AS post_id,
                       'REPOST' AS activity_type,
                       repost.actor_id AS actor_id,
                       repost.created_at AS activity_at
                FROM post_reposts repost
                INNER JOIN post_details post ON post.post_id = repost.post_id
                WHERE post.validate_status = 'APPROVED'
                  AND repost.actor_id IN (
                    SELECT following.following_id
                    FROM user_follower following
                    INNER JOIN user_follower follower_back
                      ON follower_back.follower_id = following.following_id
                     AND follower_back.following_id = :userId
                    WHERE following.follower_id = :userId
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM user_archive_items archived
                    WHERE archived.content_id = post.post_id
                      AND UPPER(archived.content_type) = 'POST'
                  )
            ) activity
            ORDER BY activity.activity_at DESC, activity.feed_entry_id DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<FriendFeedActivityProjection> findRecentFriendFeedActivities(String userId, int limit, int offset);

    @Query("""
            SELECT COUNT(*) FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
              AND (
                p.content LIKE :queryPattern
                OR p.hashtag LIKE :queryPattern
              )
            """)
    Mono<Long> countSearchApprovedPostIds(String queryPattern);

    Mono<Void> deleteByUserId(String userId);
}
