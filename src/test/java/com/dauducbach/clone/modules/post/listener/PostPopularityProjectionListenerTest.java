package com.dauducbach.clone.modules.post.listener;

import com.dauducbach.clone.modules.post.dto.event.PostEventJson;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import com.dauducbach.clone.modules.post.service.post.PostPopularityProjectionService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PostPopularityProjectionListenerTest {
    private final PostPopularityProjectionService projection = mock(PostPopularityProjectionService.class);
    private final PostPopularityProjectionListener listener = new PostPopularityProjectionListener(projection);

    private ConsumerRecord<String, String> record() {
        Instant time = Instant.now();
        var update = new PostPopularityUpdate(1, "event", "post", time, time.plusSeconds(86400), 101, "popular-v1");
        return new ConsumerRecord<>("updates", 0, 0, "post", PostEventJson.json(update));
    }

    @Test void failureMustBeThrownOnKafkaThreadForCommonErrorHandler() {
        var failure = new IllegalStateException("Redis unavailable");
        when(projection.apply(any())).thenReturn(Mono.error(failure));
        assertSame(failure, assertThrows(IllegalStateException.class, () -> listener.receive(record())));
    }

    @Test void successWaitsForProjectionCompletion() {
        AtomicBoolean completed = new AtomicBoolean();
        when(projection.apply(any())).thenReturn(Mono.delay(java.time.Duration.ofMillis(30))
                .doOnNext(value -> completed.set(true)).then());
        listener.receive(record());
        assertTrue(completed.get());
    }

    @Test void tombstoneDoesNotProject() {
        listener.receive(new ConsumerRecord<>("updates", 0, 0, "post", null));
        verifyNoInteractions(projection);
    }
}
