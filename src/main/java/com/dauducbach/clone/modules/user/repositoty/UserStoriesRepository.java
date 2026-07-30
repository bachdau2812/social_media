package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.UserStories;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface UserStoriesRepository extends ReactiveCrudRepository<UserStories, String> {
    Flux<UserStories> findByUserIdOrderByCreatedAtDesc(String userId, Pageable pageable);

    Mono<Long> countByUserId(String userId);
    Flux<UserStories> findByUserIdAndStatusOrderByCreatedAtDesc(String userId, String status, Pageable pageable);
    Mono<Long> countByUserIdAndStatus(String userId, String status);
    Mono<UserStories> findByUserIdAndPublicationIdAndPublicationOrder(String userId, String publicationId, Integer publicationOrder);

    @Query("""
            SELECT s.* FROM user_stories s
            WHERE s.user_id = :userId
              AND s.status = 'APPROVED'
              AND (s.expired_at IS NULL OR s.expired_at > CURRENT_TIMESTAMP)
            ORDER BY s.created_at DESC, s.id DESC
            LIMIT :limit OFFSET :offset
            """)
    Flux<UserStories> findActiveApprovedByUserId(String userId, int limit, long offset);

    @Query("""
            SELECT COUNT(*) FROM user_stories s
            WHERE s.user_id = :userId
              AND s.status = 'APPROVED'
              AND (s.expired_at IS NULL OR s.expired_at > CURRENT_TIMESTAMP)
            """)
    Mono<Long> countActiveApprovedByUserId(String userId);

    @Query("""
            SELECT s.* FROM user_stories s
            WHERE s.status = 'APPROVED'
              AND (s.expired_at IS NULL OR s.expired_at > CURRENT_TIMESTAMP)
              AND (
                s.user_id = :userId
                OR s.user_id IN (
                    SELECT following_id FROM user_follower WHERE follower_id = :userId
                )
              )
            ORDER BY s.created_at DESC, s.id DESC
            LIMIT :limit
            """)
    Flux<UserStories> findHomeStoryTray(String userId, int limit);
}
