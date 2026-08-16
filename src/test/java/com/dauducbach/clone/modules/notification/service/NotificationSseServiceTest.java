package com.dauducbach.clone.modules.notification.service;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationSseServiceTest {

    @Test
    void emitsAChangeOnlyToTheTargetUserAndCleansUpTheChannel() {
        NotificationSseService service = new NotificationSseService(Duration.ofHours(1));

        StepVerifier.create(service.subscribe("user-1").take(1))
                .then(() -> service.notifyChanged("user-1", "notification-1").block())
                .assertNext(event -> {
                    assertThat(event.event()).isEqualTo("notification_changed");
                    assertThat(event.data()).isEqualTo("notification-1");
                })
                .verifyComplete();

        assertThat(service.channelCount()).isZero();
    }

    @Test
    void doesNotLeakOneUsersChangeIntoAnotherUsersStream() {
        NotificationSseService service = new NotificationSseService(Duration.ofHours(1));
        Disposable userOne = service.subscribe("user-1").subscribe();

        StepVerifier.create(service.subscribe("user-2"))
                .then(() -> service.notifyChanged("user-1", "notification-1").block())
                .expectNoEvent(Duration.ofMillis(50))
                .thenCancel()
                .verify();

        userOne.dispose();
        assertThat(service.channelCount()).isZero();
    }
}
