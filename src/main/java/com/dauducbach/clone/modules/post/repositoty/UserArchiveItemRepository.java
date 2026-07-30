package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.UserArchiveItem;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface UserArchiveItemRepository extends ReactiveCrudRepository<UserArchiveItem, String> {
    @Query("SELECT * FROM user_archive_items WHERE user_id = :userId AND (:contentType IS NULL OR content_type = :contentType) ORDER BY archived_at DESC, id DESC")
    Flux<UserArchiveItem> findByUserIdAndType(String userId, String contentType);

    @Query("SELECT * FROM user_archive_items WHERE user_id = :userId AND content_id = :contentId LIMIT 1")
    Mono<UserArchiveItem> findByUserIdAndContentId(String userId, String contentId);

    @Modifying
    @Query("DELETE FROM user_archive_items WHERE user_id = :userId AND content_id = :contentId")
    Mono<Integer> restore(String userId, String contentId);

    @Modifying
    @Query("DELETE FROM user_archive_items WHERE user_id = :userId AND id = :archiveItemId")
    Mono<Integer> deleteByUserIdAndId(String userId, String archiveItemId);
}
