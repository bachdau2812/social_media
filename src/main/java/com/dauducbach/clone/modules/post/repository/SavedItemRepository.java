package com.dauducbach.clone.modules.post.repository;

import com.dauducbach.clone.modules.post.entity.SavedItem;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface SavedItemRepository extends ReactiveCrudRepository<SavedItem, String> {
    @Query("SELECT EXISTS(SELECT 1 FROM saved_items WHERE user_id = :userId AND post_id = :postId)")
    Mono<Boolean> existsByUserIdAndPostId(String userId, String postId);

    @Query("SELECT * FROM saved_items WHERE user_id = :userId AND post_id = :postId LIMIT 1")
    Mono<SavedItem> findSavedItemByUserIdAndPostId(String userId, String postId);

    @Query("SELECT * FROM saved_items WHERE user_id = :userId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<SavedItem> findByUserId(String userId, int limit, int offset);

    @Query("SELECT COUNT(*) FROM saved_items WHERE user_id = :userId")
    Mono<Long> countByUserId(String userId);

    @Modifying
    @Query("DELETE FROM saved_items WHERE user_id = :userId AND post_id = :postId")
    Mono<Integer> deleteByUserIdAndPostId(String userId, String postId);
}
