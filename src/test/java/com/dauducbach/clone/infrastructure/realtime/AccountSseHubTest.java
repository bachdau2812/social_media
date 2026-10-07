package com.dauducbach.clone.infrastructure.realtime;

import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AccountSseHubTest {

    @Test
    void publishesAnEventToEveryOpenTabForTheSameUser() {
        AccountSseHub hub = new AccountSseHub();
        StepVerifier.create(hub.subscribe("user-1").take(1))
                .then(() -> StepVerifier.create(hub.subscribe("user-1").take(1))
                        .then(() -> hub.sendToUser("user-1", "post_upload", "payload").block())
                        .assertNext(AccountSseHubTest::assertUploadEvent)
                        .verifyComplete())
                .assertNext(AccountSseHubTest::assertUploadEvent)
                .verifyComplete();
    }

    @Test
    void ignoresBlankUserIds() {
        AccountSseHub hub = new AccountSseHub();

        StepVerifier.create(hub.sendToUser(" ", "post_upload", "payload"))
                .verifyComplete();
    }

    @Test
    void subscriptionRegistrationAndPublishingAreLazy() {
        AccountSseHub hub = new AccountSseHub();
        var stream = hub.subscribe("user-1");
        AtomicReference<ServerSentEvent<String>> received = new AtomicReference<>();

        assertThat(hub.channelCount()).isZero();
        var subscription = stream.subscribe(received::set);
        assertThat(hub.channelCount()).isEqualTo(1);

        var publish = hub.sendToUser("user-1", "post_upload", "payload");
        assertThat(received.get()).isNull();
        publish.block();
        assertUploadEvent(received.get());

        subscription.dispose();
        assertThat(hub.channelCount()).isZero();
    }

    private static void assertUploadEvent(ServerSentEvent<String> event) {
        assertThat(event.event()).isEqualTo("post_upload");
        assertThat(event.data()).isEqualTo("payload");
    }
}
