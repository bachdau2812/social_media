package com.dauducbach.clone.modules.post.service;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ImageScanWorkerTest {

    @Test
    void kafkaCompletionWaitsForPostModerationFlow() {
        PostMediaModerationOrchestrator postOrchestrator = mock(PostMediaModerationOrchestrator.class);
        CommentMediaModerationOrchestrator commentOrchestrator = mock(CommentMediaModerationOrchestrator.class);
        ImageScanWorker worker = new ImageScanWorker(postOrchestrator, commentOrchestrator);
        Sinks.Empty<Void> completion = Sinks.empty();
        when(postOrchestrator.process(org.mockito.ArgumentMatchers.eq("post-1"),
                org.mockito.ArgumentMatchers.eq("user-1"), anyList()))
                .thenReturn(completion.asMono());

        CompletableFuture<Void> result = worker.handlePostScanEvent("""
                {"postId":"post-1","userId":"user-1","items":[
                  {"orderNumber":1,"secureUrl":"https://cdn/item.jpg","publicId":"item-1"}
                ]}
                """);

        assertThat(result).isNotDone();
        completion.tryEmitEmpty();
        result.join();
        verify(postOrchestrator).process(org.mockito.ArgumentMatchers.eq("post-1"),
                org.mockito.ArgumentMatchers.eq("user-1"), anyList());
    }

    @Test
    void invalidCommentPayloadDoesNotStartModeration() {
        PostMediaModerationOrchestrator postOrchestrator = mock(PostMediaModerationOrchestrator.class);
        CommentMediaModerationOrchestrator commentOrchestrator = mock(CommentMediaModerationOrchestrator.class);
        ImageScanWorker worker = new ImageScanWorker(postOrchestrator, commentOrchestrator);

        worker.handleCommentScanEvent("{\"commentId\":\"\",\"postId\":\"post-1\",\"media\":[]}").join();

        verify(commentOrchestrator, never()).process(
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.anyString(),
                anyList());
    }
}
