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

    @Query("SELECT * FROM post_details WHERE user_id = :userId ORDER BY post_id DESC LIMIT :limit OFFSET :offset")
    Flux<PostDetails> findByUserId(String userId, int limit, int offset);

    Flux<PostDetails> findAllByUserId(String userId);

    @Query("""
            SELECT post_id FROM post_details
            WHERE validate_status = 'APPROVED'
              AND (
                LOWER(COALESCE(content, '')) LIKE CONCAT('%', LOWER(:query), '%')
                OR LOWER(COALESCE(hashtag, '')) LIKE CONCAT('%', LOWER(:query), '%')
              )
            ORDER BY created_at DESC, post_id DESC
            LIMIT :#{#pageable.pageSize} OFFSET :#{#pageable.offset}
            """)
    Flux<String> searchApprovedPostIds(String query, Pageable pageable);

    @Query("""
            SELECT * FROM post_details
            WHERE validate_status = 'APPROVED'
            ORDER BY created_at DESC, post_id DESC
            LIMIT :limit
            """)
    Flux<PostDetails> findRecentApprovedPosts(int limit);

    @Query("""
            SELECT COUNT(*) FROM post_details
            WHERE validate_status = 'APPROVED'
              AND (
                LOWER(COALESCE(content, '')) LIKE CONCAT('%', LOWER(:query), '%')
                OR LOWER(COALESCE(hashtag, '')) LIKE CONCAT('%', LOWER(:query), '%')
              )
            """)
    Mono<Long> countSearchApprovedPostIds(String query);

    Mono<Void> deleteByUserId(String userId);
}

