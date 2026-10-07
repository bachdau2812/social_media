package com.dauducbach.clone.modules.post.repository.story;

import com.dauducbach.clone.modules.post.repository.story.projection.StoryViewerRow;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class StoryViewQueryRepository {
    private final DatabaseClient databaseClient;

    public Flux<StoryViewerRow> findViewerPage(String storyId, int limit, int offset) {
        return databaseClient.sql("""
                        SELECT viewer_id, reaction, viewed_at
                        FROM story_views
                        WHERE story_id = :storyId
                        ORDER BY viewed_at DESC, viewer_id ASC
                        LIMIT :limit OFFSET :offset
                        """)
                .bind("storyId", storyId)
                .bind("limit", limit)
                .bind("offset", offset)
                .map((row, metadata) -> new StoryViewerRow(
                        row.get("viewer_id", String.class),
                        row.get("reaction", String.class),
                        row.get("viewed_at", Instant.class)))
                .all();
    }
}
