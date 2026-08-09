package com.dauducbach.clone.modules.post.service.story;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class DirectStoryLikeContractTest {
    private static final Path MAIN_JAVA = Path.of("src/main/java");
    private static final Path SERVICE = MAIN_JAVA.resolve(
            "com/dauducbach/clone/modules/post/service/story/StoryReactionService.java");
    private static final Path STORY_SCHEMA = Path.of(
            "src/main/resources/db/manual/story_reply_schema.sql");
    private static final Path CLEANUP_MIGRATION = Path.of(
            "src/main/resources/db/manual/remove_story_like_outbox.sql");

    @Test
    void storyLikesPublishDirectlyWithoutProductionOutboxCode() throws Exception {
        String service = Files.readString(SERVICE);

        assertThat(service).contains("KafkaSender", "publishLikeEvent", "LIKE_EVENT_TOPIC");
        assertThat(findProductionSourcesContaining("StoryLikeOutbox")).isEmpty();
    }

    @Test
    void publishLifecycleLogsCarryTheCompleteStoryLikeContext() throws Exception {
        String service = Files.readString(SERVICE).replaceAll("\\s+", " ");

        assertThat(service).contains(
                "|publishLikeEvent|sending|storyId={}|actorId={}|ownerId={}|interactionId={}",
                "|publishLikeEvent|brokerAcknowledged|storyId={}|actorId={}|ownerId={}|interactionId={}|topic={}|partition={}|offset={}",
                "|publishLikeEvent|failed|storyId={}|actorId={}|ownerId={}|interactionId={}|errorType={}");
    }

    @Test
    void storySchemaKeepsDeduplicationWithoutCreatingOrSeedingOutbox() throws Exception {
        String schema = Files.readString(STORY_SCHEMA);

        assertThat(schema).contains("reaction_interaction_id");
        assertThat(schema).contains("uk_notification_events_story_like_dedup");
        assertThat(schema).doesNotContain("CREATE TABLE story_like_outbox");
        assertThat(schema).doesNotContain("INSERT INTO story_like_outbox");
    }

    @Test
    void cleanupMigrationDropsObsoleteOutboxIdempotently() throws Exception {
        assertThat(CLEANUP_MIGRATION).exists();
        assertThat(Files.readString(CLEANUP_MIGRATION).replace("\r\n", "\n"))
                .isEqualTo("""
                        -- Remove the obsolete Story Like notification outbox after deploying direct Kafka publishing.
                        DROP TABLE IF EXISTS story_like_outbox;
                        """);
    }

    private static Stream<Path> findProductionSourcesContaining(String value) throws IOException {
        return Files.walk(MAIN_JAVA)
                .filter(path -> path.toString().endsWith(".java"))
                .filter(path -> {
                    try {
                        return Files.readString(path).contains(value);
                    } catch (IOException exception) {
                        throw new IllegalStateException("Cannot read " + path, exception);
                    }
                });
    }
}
