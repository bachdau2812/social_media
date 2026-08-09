package com.dauducbach.clone.modules.post.service.story;

import com.dauducbach.clone.modules.post.entity.story.StoryLikeOutboxEntry;
import com.dauducbach.clone.modules.post.repositoty.story.StoryLikeOutboxRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StoryLikeOutboxPublisherTest {
    @Mock StoryLikeOutboxRepository repository;
    @Mock KafkaSender<String, String> kafkaSender;
    @Mock SenderResult<String> senderResult;
    @Mock RecordMetadata recordMetadata;

    @Test
    void brokerAcknowledgementDeletesTheLeasedOutboxRow() {
        StoryLikeOutboxEntry entry = entry();
        when(repository.leaseDue(anyString(), any(Instant.class), eq(50))).thenReturn(Flux.just(entry));
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> records = invocation.getArgument(0);
            return Flux.from(records)
                    .doOnNext(this::assertPayload)
                    .map(ignored -> senderResult);
        });
        when(senderResult.exception()).thenReturn(null);
        when(senderResult.recordMetadata()).thenReturn(recordMetadata);
        when(recordMetadata.topic()).thenReturn("like_event");
        when(recordMetadata.partition()).thenReturn(0);
        when(recordMetadata.offset()).thenReturn(104L);
        when(repository.acknowledge(eq("interaction-1"), anyString())).thenReturn(Mono.just(1));

        StepVerifier.create(new StoryLikeOutboxPublisher(repository, kafkaSender).publishDue())
                .verifyComplete();

        verify(repository).acknowledge(eq("interaction-1"), anyString());
        verify(repository, never()).retry(anyString(), anyString(), any(Instant.class));
    }

    @Test
    void producerFailureRetainsTheRowAndSchedulesRetry() {
        when(repository.leaseDue(anyString(), any(Instant.class), eq(50))).thenReturn(Flux.just(entry()));
        when(kafkaSender.send(any(Publisher.class)))
                .thenReturn(Flux.error(new IllegalStateException("Kafka unavailable")));
        when(repository.retry(eq("interaction-1"), anyString(), any(Instant.class))).thenReturn(Mono.just(1));

        StepVerifier.create(new StoryLikeOutboxPublisher(repository, kafkaSender).publishDue())
                .verifyComplete();

        verify(repository).retry(eq("interaction-1"), anyString(), any(Instant.class));
        verify(repository, never()).acknowledge(anyString(), anyString());
    }

    private void assertPayload(SenderRecord<String, String, String> record) {
        assertThat(record.topic()).isEqualTo("like_event");
        assertThat(record.key()).isEqualTo("story-1");
        JsonObject payload = GsonUtils.fromString(record.value());
        assertThat(payload.get("actorId").getAsString()).isEqualTo("actor-1");
        assertThat(payload.get("targetId").getAsString()).isEqualTo("story-1");
        assertThat(payload.get("targetType").getAsString()).isEqualTo("STORY");
        assertThat(payload.get("targetOwnerId").getAsString()).isEqualTo("owner-1");
        assertThat(payload.get("interactionId").getAsString()).isEqualTo("interaction-1");
    }

    private StoryLikeOutboxEntry entry() {
        return StoryLikeOutboxEntry.builder()
                .interactionId("interaction-1")
                .storyId("story-1")
                .actorId("actor-1")
                .ownerId("owner-1")
                .createdAt(Instant.parse("2026-08-09T14:15:00Z"))
                .attemptCount(0)
                .nextAttemptAt(Instant.parse("2026-08-09T14:15:00Z"))
                .leaseToken("lease-1")
                .build();
    }
}
