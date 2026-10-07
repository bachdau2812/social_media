package com.dauducbach.clone.integration.storyviewers;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.repository.story.projection.StoryViewerRow;
import com.dauducbach.clone.modules.post.stories.library.StoryViewerSearch;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;

/**
 * Read-only cross-module projection. Keep SQL schema coupling here, outside story
 * business logic; identities/avatars and follow actions still use public contracts.
 */
@Repository
@RequiredArgsConstructor
public class StoryViewerSearchAdapter implements StoryViewerSearch {
    private static final String MATCHING_VIEWERS = """
            FROM story_views views
            JOIN user_details identity ON identity.user_id = views.viewer_id
            WHERE views.story_id = :storyId
              AND (LOCATE(LOWER(:query), LOWER(COALESCE(identity.username, ''))) > 0
                OR LOCATE(LOWER(:query), LOWER(COALESCE(identity.full_name, ''))) > 0)
            """;
    private final DatabaseClient databaseClient;

    @Override
    public Mono<PageResponse<StoryViewerRow>> search(String storyId, String query, int page, int size, int offset) {
        var content = databaseClient.sql("SELECT views.viewer_id, views.reaction, views.viewed_at " + MATCHING_VIEWERS + """
                        ORDER BY views.viewed_at DESC, views.viewer_id ASC
                        LIMIT :limit OFFSET :offset
                        """)
                .bind("storyId", storyId).bind("query", query)
                .bind("limit", size).bind("offset", offset)
                .map((row, metadata) -> new StoryViewerRow(row.get("viewer_id", String.class),
                        row.get("reaction", String.class), row.get("viewed_at", Instant.class)))
                .all().collectList();
        var count = databaseClient.sql("SELECT COUNT(*) AS total " + MATCHING_VIEWERS)
                .bind("storyId", storyId).bind("query", query)
                .map((row, metadata) -> row.get("total", Long.class)).one().defaultIfEmpty(0L);
        return Mono.zip(content, count).map(result -> PageResponse.of(result.getT1(), page, result.getT2(), size));
    }
}