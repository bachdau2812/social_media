package com.dauducbach.clone.modules.notification.controller;

import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.dto.response.PushTokenRegisterResponse;
import com.dauducbach.clone.modules.notification.service.PushNotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NotificationControllerTest {
    PushNotificationService pushNotificationService;
    WebTestClient client;

    @BeforeEach
    void setUp() {
        pushNotificationService = mock(PushNotificationService.class);
        client = WebTestClient.bindToController(new NotificationController(pushNotificationService)).build();
    }

    @Test
    void registerPushTokenReturnsApiResponse() {
        when(pushNotificationService.registerPushToken(any(PushTokenRegisterRequest.class)))
                .thenReturn(Mono.just(new PushTokenRegisterResponse(
                        "token-id-1",
                        "user-1",
                        "device-1",
                        Instant.parse("2026-06-14T00:00:00Z")
                )));

        client.post()
                .uri("/notifications/push-tokens")
                .bodyValue(new PushTokenRegisterRequest("user-1", "device-1", "token-1"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.message").isEqualTo("Push token registered successfully")
                .jsonPath("$.result.id").isEqualTo("token-id-1")
                .jsonPath("$.result.userId").isEqualTo("user-1")
                .jsonPath("$.result.deviceId").isEqualTo("device-1");
    }
}
