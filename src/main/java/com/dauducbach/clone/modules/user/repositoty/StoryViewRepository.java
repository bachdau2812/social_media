package com.dauducbach.clone.modules.user.repositoty;

import com.dauducbach.clone.modules.user.entity.StoryView;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.r2dbc.repository.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface StoryViewRepository extends ReactiveCrudRepository<StoryView, String> {
    Mono<StoryView> findByStoryIdAndViewerId(String storyId, String viewerId);
    Mono<Long> countByStoryId(String storyId);

    @Query("SELECT story_id FROM story_views WHERE viewer_id = :viewerId AND story_id IN (:storyIds)")
    Flux<String> findViewedStoryIds(String viewerId, Collection<String> storyIds);

    @Query("SELECT * FROM story_views WHERE viewer_id = :viewerId AND story_id IN (:storyIds)")
    Flux<StoryView> findByViewerIdAndStoryIdIn(String viewerId, Collection<String> storyIds);

    @Query("""
            UPDATE story_views
            SET reaction = 'LIKE', viewed_at = CURRENT_TIMESTAMP(6)
            WHERE story_id = :storyId
              AND viewer_id = :viewerId
              AND (reaction IS NULL OR reaction <> 'LIKE')
            """)
    Mono<Integer> markLiked(String storyId, String viewerId);

    @Query("""
            UPDATE story_views
            SET reaction = NULL
            WHERE story_id = :storyId
              AND viewer_id = :viewerId
              AND reaction = 'LIKE'
            """)
    Mono<Integer> clearLike(String storyId, String viewerId);
}
