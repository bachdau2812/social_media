package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.SavedItem;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface SavedItemRepository extends ReactiveCrudRepository<SavedItem, String> {
    @Query("SELECT * FROM saved_items WHERE user_id = :userId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<SavedItem> findByUserId(String userId, int limit, int offset);

    @Query("SELECT COUNT(*) FROM saved_items WHERE user_id = :userId")
    Mono<Long> countByUserId(String userId);

    @Modifying
    @Query("DELETE FROM saved_items WHERE user_id = :userId AND post_id = :postId")
    Mono<Integer> deleteByUserIdAndPostId(String userId, String postId);
}
