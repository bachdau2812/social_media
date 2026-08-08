package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.service.SseRealtimeFanoutPublisher;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostSseServiceTest {

    @Test
    void sendCompletionWaitsForRedisFanout() {
        SseRealtimeFanoutPublisher publisher = mock(SseRealtimeFanoutPublisher.class);
        PostSseService service = new PostSseService(publisher);
        Sinks.Empty<Void> completion = Sinks.empty();
        when(publisher.publish("user-1", "post_upload", "payload"))
                .thenReturn(completion.asMono());

        CompletableFuture<Void> result = service
                .sendToUser("user-1", "post_upload", "payload")
                .toFuture();

        assertThat(result).isNotDone();
        completion.tryEmitEmpty();
        result.join();
        verify(publisher).publish("user-1", "post_upload", "payload");
    }
}
