package com.dauducbach.clone.modules.post.service.story;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class StoryReactionOutboxContractTest {
    private static final Path SERVICE = Path.of(
            "src/main/java/com/dauducbach/clone/modules/post/service/story/StoryReactionService.java");

    @Test
    void commitsReactionAndOutboxTogetherWithoutPublishingKafkaFromHttpRequest() throws Exception {
        String source = Files.readString(SERVICE);

        assertThat(source).contains("StoryLikeOutboxRepository");
        assertThat(source).contains("TransactionalOperator");
        assertThat(source).contains("outboxRepository.enqueue");
        assertThat(source).contains("transactionalOperator.transactional");
        assertThat(source).doesNotContain("KafkaSender", "publishLikeEvent(");
    }
}
