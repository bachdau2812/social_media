package com.dauducbach.clone.modules.post.repositoty.story;

import com.dauducbach.clone.modules.post.entity.story.StoryView;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collection;

public interface StoryViewRepository extends ReactiveCrudRepository<StoryView, String> {
    Mono<StoryView> findByStoryIdAndViewerId(String storyId, String viewerId);
    Mono<Long> countByStoryId(String storyId);

    @Modifying
    @Query("""
            INSERT INTO story_views (id, story_id, viewer_id, reaction, viewed_at)
            VALUES (:id, :storyId, :viewerId, :reaction, :viewedAt)
            ON DUPLICATE KEY UPDATE
              viewed_at = :viewedAt,
              reaction = COALESCE(:reaction, reaction)
            """)
    Mono<Integer> upsertView(
            String id,
            String storyId,
            String viewerId,
            String reaction,
            Instant viewedAt
    );

    @Query("SELECT story_id FROM story_views WHERE viewer_id = :viewerId AND story_id IN (:storyIds)")
    Flux<String> findViewedStoryIds(String viewerId, Collection<String> storyIds);

    @Query("SELECT * FROM story_views WHERE viewer_id = :viewerId AND story_id IN (:storyIds)")
    Flux<StoryView> findByViewerIdAndStoryIdIn(String viewerId, Collection<String> storyIds);

    @Query("""
            UPDATE story_views
            SET reaction = 'LIKE',
                reaction_interaction_id = :interactionId,
                viewed_at = CURRENT_TIMESTAMP(6)
            WHERE story_id = :storyId
              AND viewer_id = :viewerId
              AND (reaction IS NULL OR reaction <> 'LIKE')
            """)
    Mono<Integer> markLiked(String storyId, String viewerId, String interactionId);

    @Query("""
            UPDATE story_views
            SET reaction_interaction_id = :interactionId
            WHERE story_id = :storyId
              AND viewer_id = :viewerId
              AND reaction = 'LIKE'
              AND (reaction_interaction_id IS NULL OR reaction_interaction_id = '')
            """)
    Mono<Integer> assignLikeInteractionIdIfMissing(String storyId, String viewerId, String interactionId);

    @Query("""
            UPDATE story_views
            SET reaction = NULL,
                reaction_interaction_id = NULL
            WHERE story_id = :storyId
              AND viewer_id = :viewerId
              AND reaction = 'LIKE'
            """)
    Mono<Integer> clearLike(String storyId, String viewerId);
}
