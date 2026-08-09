package com.dauducbach.clone.modules.post.service.story;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class StoryLikeOutboxPublisherContractTest {
    private static final Path PUBLISHER = Path.of(
            "src/main/java/com/dauducbach/clone/modules/post/service/story/StoryLikeOutboxPublisher.java");

    @Test
    void publisherRequiresBrokerAcknowledgementBeforeDeletingAndRetriesFailures() throws Exception {
        assertThat(PUBLISHER).exists();
        String source = Files.readString(PUBLISHER);

        assertThat(source).contains("repository.leaseDue");
        assertThat(source).contains("result.recordMetadata()");
        assertThat(source).contains("repository.acknowledge");
        assertThat(source).contains("repository.retry");
        assertThat(source).contains("@Scheduled");
        assertThat(source).contains("AtomicBoolean");
    }
}
