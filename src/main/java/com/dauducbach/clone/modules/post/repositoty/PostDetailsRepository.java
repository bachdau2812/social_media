package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface PostDetailsRepository extends ReactiveCrudRepository<PostDetails, String> {
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
            SELECT p.post_id FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
              AND (
                LOWER(COALESCE(p.content, '')) LIKE CONCAT('%', LOWER(:query), '%')
                OR LOWER(COALESCE(p.hashtag, '')) LIKE CONCAT('%', LOWER(:query), '%')
              )
            ORDER BY p.created_at DESC, p.post_id DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<String> searchApprovedPostIds(String query, Pageable pageable);

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
            SELECT COUNT(*) FROM post_details p
            WHERE p.validate_status = 'APPROVED'
              AND NOT EXISTS (
                SELECT 1 FROM user_archive_items archived
                WHERE archived.content_id = p.post_id
                  AND UPPER(archived.content_type) = 'POST'
              )
              AND (
                LOWER(COALESCE(p.content, '')) LIKE CONCAT('%', LOWER(:query), '%')
                OR LOWER(COALESCE(p.hashtag, '')) LIKE CONCAT('%', LOWER(:query), '%')
              )
            """)
    Mono<Long> countSearchApprovedPostIds(String query);

    Mono<Void> deleteByUserId(String userId);
}
