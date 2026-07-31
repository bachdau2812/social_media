package com.dauducbach.clone.modules.notification.service;

import com.google.firebase.FirebaseApp;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class FirebaseNotificationPushGatewayTest {

    @Test
    void reportsActionableConfigurationErrorWhenFirebaseIsNotInitialized() {
        assertThat(FirebaseApp.getApps()).isEmpty();
        FirebaseNotificationPushGateway gateway = new FirebaseNotificationPushGateway();
        NotificationPushPayload payload = new NotificationPushPayload(
                "device-token",
                "Story notification",
                "A liked your story",
                Map.of("url", "/story/owner-1/story-1"),
                "LIKE_STORY:interaction-1");

        StepVerifier.create(gateway.send(payload))
                .expectErrorMatches(error -> error instanceof IllegalStateException
                        && error.getMessage().contains("FIREBASE_ENABLED=true")
                        && error.getMessage().contains("FIREBASE_CREDENTIALS_PATH"))
                .verify();
    }
}
