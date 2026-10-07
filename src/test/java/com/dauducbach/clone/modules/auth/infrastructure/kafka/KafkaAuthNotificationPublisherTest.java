package com.dauducbach.clone.modules.auth.infrastructure.kafka;

import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaAuthNotificationPublisherTest {
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void profileCreationEventContainsProfileDataButNeverTheCredential() {
        KafkaSender<String, String> sender = mock(KafkaSender.class);
        when(sender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> records = invocation.getArgument(0);
            return Flux.from(records).doOnNext(record -> {
                assertThat(record.topic()).isEqualTo("profile_creation_event");
                var payload = GsonUtils.fromString(record.value());
                assertThat(payload.get("userId").getAsString()).isEqualTo("user-1");
                assertThat(payload.get("username").getAsString()).isEqualTo("alice");
                assertThat(payload.get("password")).isNull();
                assertThat(payload.getAsJsonArray("hobbyList")).hasSize(1);
            }).thenMany(Flux.empty());
        });
        var publisher = new KafkaAuthNotificationPublisher(sender);
        RegistrationDraft draft = new RegistrationDraft(
                "Alice Example", "alice", "secret-password", "alice@example.com", null, null,
                null, null, null, List.of("music"), "USER");

        StepVerifier.create(publisher.publishProfileCreated(draft, "user-1")).verifyComplete();
    }
}
