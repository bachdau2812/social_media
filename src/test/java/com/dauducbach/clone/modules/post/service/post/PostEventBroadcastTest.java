package com.dauducbach.clone.modules.post.service.post;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostEventBroadcastTest {
    @Test void captionOnlyEventMustStartARebuild() {
        PostVectorService vectorService = mock(PostVectorService.class, invocation -> reactor.core.publisher.Mono.never());
        CompletableFuture<Void> result = new PostEventBroadcast(vectorService).handlePostEmbeddingEvent("{\"post_id\":\"p\"}");
        assertThat(result).isNotDone();
        result.cancel(true);
    }

    @Test
    void kafkaCompletionWaitsForEmbeddingFlow() {
        PostVectorService vectorService = mock(PostVectorService.class);
        PostEventBroadcast listener = new PostEventBroadcast(vectorService);
        Sinks.Empty<Void> completion = Sinks.empty();
        when(vectorService.rebuild("post-1"))
                .thenReturn(completion.asMono());

        CompletableFuture<Void> result = listener.handlePostEmbeddingEvent(
                "{\"post_id\":\"post-1\",\"content\":\"caption\"}");

        assertThat(result).isNotDone();
        completion.tryEmitEmpty();
        result.join();
        verify(vectorService).rebuild("post-1");
    }

    @Test void updateEventsAlsoRebuildAndProviderFailureRejectsAcknowledgement() throws Exception {
        var annotation = PostEventBroadcast.class.getMethod("handlePostEmbeddingEvent", String.class)
                .getAnnotation(org.springframework.kafka.annotation.KafkaListener.class);
        assertThat(annotation.topics()).contains("post_update_event");
        PostVectorService service = mock(PostVectorService.class);
        when(service.rebuild("p")).thenReturn(reactor.core.publisher.Mono.error(new IllegalStateException("provider")));
        assertThat(new PostEventBroadcast(service).handlePostEmbeddingEvent("{\"postId\":\"p\"}"))
                .isCompletedExceptionally();
    }
}
