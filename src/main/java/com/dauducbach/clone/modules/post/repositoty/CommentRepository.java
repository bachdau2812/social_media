package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.Comment;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface CommentRepository extends ReactiveCrudRepository<Comment, String> {
    @Query("SELECT * FROM comments WHERE post_id = :postId AND parent_id IS NULL ORDER BY timestamp DESC LIMIT :limit OFFSET :offset")
    Flux<Comment> findRootByPostId(String postId, int limit, int offset);

    @Query("SELECT * FROM comments WHERE parent_id = :parentId ORDER BY timestamp DESC LIMIT :limit OFFSET :offset")
    Flux<Comment> findByParentId(String parentId, int limit, int offset);

    @Query("SELECT * FROM comments WHERE post_id = :postId ORDER BY timestamp DESC")
    Flux<Comment> findByPostId(String postId);

    @Query("SELECT DISTINCT user_id FROM comments WHERE post_id = :postId")
    Flux<String> findDistinctUserIdsByPostId(String postId);

    @Query("SELECT * FROM comments WHERE user_id = :userId ORDER BY timestamp DESC LIMIT :limit OFFSET :offset")
    Flux<Comment> findByUserId(String userId, int limit, int offset);

    @Query("SELECT COUNT(*) FROM comments WHERE user_id = :userId")
    Mono<Long> countByUserId(String userId);

    @Query("SELECT COUNT(*) FROM comments WHERE post_id = :postId AND parent_id IS NULL")
    Mono<Long> countRootByPostId(String postId);
    @Query("SELECT COUNT(*) FROM comments WHERE post_id = :postId")
    Mono<Long> countByPostId(String postId);

    @Query("SELECT COUNT(*) FROM comments WHERE parent_id = :parentId")
    Mono<Long> countByParentId(String parentId);

    @Query("""
            SELECT post_id
            FROM comments
            WHERE user_id = :userId
            GROUP BY post_id
            ORDER BY MAX(timestamp) DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<String> findCommentedPostIdsByUserId(String userId, int limit, int offset);

    @Query("SELECT COUNT(DISTINCT post_id) FROM comments WHERE user_id = :userId")
    Mono<Long> countCommentedPostsByUserId(String userId);

    Mono<Void> deleteById(String id);
}
