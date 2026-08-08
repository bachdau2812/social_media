package com.dauducbach.clone.modules.post.service.post;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostEventBroadcastTest {

    @Test
    void kafkaCompletionWaitsForEmbeddingFlow() {
        PostVectorService vectorService = mock(PostVectorService.class);
        PostEventBroadcast listener = new PostEventBroadcast(vectorService);
        Sinks.Empty<Void> completion = Sinks.empty();
        when(vectorService.processPostEmbedding("post-1", "caption"))
                .thenReturn(completion.asMono());

        CompletableFuture<Void> result = listener.handlePostEmbeddingEvent(
                "{\"post_id\":\"post-1\",\"content\":\"caption\"}");

        assertThat(result).isNotDone();
        completion.tryEmitEmpty();
        result.join();
        verify(vectorService).processPostEmbedding("post-1", "caption");
    }
}

