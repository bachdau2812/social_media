package com.dauducbach.clone.modules.user.profile.infrastructure.kafka;

import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AvatarMediaKafkaPublisherTest {
    @Mock
    KafkaSender<String, String> kafkaSender;

    @Test
    void keepsAvatarScanTopicKeyAndPayloadCompatible() {
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> records = invocation.getArgument(0);
            return Flux.from(records).doOnNext(record -> {
                assertThat(record.topic()).isEqualTo("check_avatar_media_event");
                assertThat(record.key()).isEqualTo("user-1");
                JsonObject payload = GsonUtils.fromString(record.value());
                assertThat(payload.get("userId").getAsString()).isEqualTo("user-1");
                assertThat(payload.get("avatarUrl").getAsString()).isEqualTo("https://cdn.example/avatar.jpg");
                assertThat(payload.get("publicId").getAsString()).isEqualTo("avatar");
            }).thenMany(Flux.empty());
        });

        StepVerifier.create(new AvatarMediaKafkaPublisher(kafkaSender)
                        .requestAvatarScan("user-1", "https://cdn.example/avatar.jpg", "avatar"))
                .verifyComplete();
    }

    @Test
    void keepsAvatarUpdateTopicKeyAndPayloadCompatible() {
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> records = invocation.getArgument(0);
            return Flux.from(records).doOnNext(record -> {
                assertThat(record.topic()).isEqualTo("avatar_update_event");
                assertThat(record.key()).isEqualTo("user-1");
                JsonObject payload = GsonUtils.fromString(record.value());
                assertThat(payload.get("userId").getAsString()).isEqualTo("user-1");
                assertThat(payload.get("avatarUrl").getAsString()).isEqualTo("https://cdn.example/avatar.jpg");
                assertThat(payload.get("mediaId").getAsString()).isEqualTo("media-1");
            }).thenMany(Flux.empty());
        });

        StepVerifier.create(new AvatarMediaKafkaPublisher(kafkaSender)
                        .publishAvatarUpdated("user-1", "https://cdn.example/avatar.jpg", "media-1"))
                .verifyComplete();
    }
}
