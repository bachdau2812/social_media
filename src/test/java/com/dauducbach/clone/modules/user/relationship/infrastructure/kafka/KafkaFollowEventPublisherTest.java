package com.dauducbach.clone.modules.user.relationship.infrastructure.kafka;

import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaFollowEventPublisherTest {
    @Mock
    KafkaSender<String, String> kafkaSender;

    @Test
    void publishesExistingFollowEventContract() {
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> records = invocation.getArgument(0);
            return Flux.from(records).doOnNext(record -> {
                assertThat(record.topic()).isEqualTo("follow_event");
                assertThat(record.key()).isEqualTo("follower-1");
                assertPayload(record.value());
            }).thenMany(Flux.empty());
        });

        StepVerifier.create(new KafkaFollowEventPublisher(kafkaSender).followed("follower-1", "owner-1"))
                .verifyComplete();
    }

    @Test
    void publishesExistingUnfollowEventContract() {
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> records = invocation.getArgument(0);
            return Flux.from(records).doOnNext(record -> {
                assertThat(record.topic()).isEqualTo("un_follow_event");
                assertThat(record.key()).isEqualTo("follower-1");
                assertPayload(record.value());
            }).thenMany(Flux.empty());
        });

        StepVerifier.create(new KafkaFollowEventPublisher(kafkaSender).unfollowed("follower-1", "owner-1"))
                .verifyComplete();
    }

    private void assertPayload(String value) {
        JsonObject payload = GsonUtils.fromString(value);
        assertThat(payload.get("followerId").getAsString()).isEqualTo("follower-1");
        assertThat(payload.get("followingId").getAsString()).isEqualTo("owner-1");
    }
}
