package com.dauducbach.clone.modules.post.repositoty.story;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class StoryLikeOutboxRepositoryContractTest {
    private static final Path REPOSITORY = Path.of(
            "src/main/java/com/dauducbach/clone/modules/post/repositoty/story/StoryLikeOutboxRepository.java");
    private static final Path MIGRATION = Path.of("src/main/resources/db/manual/story_reply_schema.sql");

    @Test
    void outboxRepositoryUsesIdempotentEnqueueAndTokenCheckedLeaseLifecycle() throws Exception {
        assertThat(REPOSITORY).exists();
        String source = Files.readString(REPOSITORY);

        assertThat(source).contains("ON DUPLICATE KEY UPDATE interaction_id = interaction_id");
        assertThat(source).contains("lease_until IS NULL OR lease_until < CURRENT_TIMESTAMP(6)");
        assertThat(source).contains("lease_token = :leaseToken");
        assertThat(source).contains("DELETE FROM story_like_outbox");
        assertThat(source).contains("AND lease_token = :leaseToken");
        assertThat(source).contains("attempt_count = attempt_count + 1");
        assertThat(source).contains("lease_token = NULL", "lease_until = NULL");
    }

    @Test
    void migrationCreatesAndSeedsOnlyDurablyIdentifiedMissingStoryLikes() throws Exception {
        String sql = Files.readString(MIGRATION);

        assertThat(sql).contains("CREATE TABLE story_like_outbox");
        assertThat(sql).contains("PRIMARY KEY (interaction_id)");
        assertThat(sql).contains("idx_story_like_outbox_due");
        assertThat(sql).contains("sv.reaction_interaction_id IS NOT NULL");
        assertThat(sql).contains("ne.dedup_key = CONCAT('LIKE_STORY:', sv.reaction_interaction_id, ':', us.user_id)");
        assertThat(sql).contains("NOT EXISTS");
    }
}
