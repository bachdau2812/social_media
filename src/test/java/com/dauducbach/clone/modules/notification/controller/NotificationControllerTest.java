package com.dauducbach.clone.modules.notification.controller;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.dto.response.PushTokenRegisterResponse;
import com.dauducbach.clone.modules.notification.service.PushTokenRegistrationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Mono;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationControllerTest {
    private PushTokenRegistrationService pushTokenRegistrationService;
    private NotificationController controller;
    private Authentication authentication;

    @BeforeEach
    void setUp() {
        pushTokenRegistrationService = mock(PushTokenRegistrationService.class);
        controller = new NotificationController(pushTokenRegistrationService);
        authentication = mock(Authentication.class);
        when(authentication.getName()).thenReturn("user-1");
    }

    @Test
    void registerPushTokenReturnsApiResponse() {
        when(pushTokenRegistrationService.registerPushToken(any(PushTokenRegisterRequest.class)))
                .thenReturn(Mono.just(new PushTokenRegisterResponse(
                        "token-id-1",
                        "user-1",
                        "device-1",
                        Instant.parse("2026-06-14T00:00:00Z")
                )));

        var response = controller.registerPushToken(
                new PushTokenRegisterRequest("user-1", "device-1", "token-1"), authentication).block();

        assertEquals("Push token registered successfully", response.getMessage());
        assertEquals("token-id-1", response.getResult().id());
        verify(pushTokenRegistrationService).registerPushToken(any(PushTokenRegisterRequest.class));
    }

    @Test
    void registerPushTokenRejectsAnActorDifferentFromTheAuthenticatedUser() {
        assertThrows(AppException.class, () -> controller.registerPushToken(
                new PushTokenRegisterRequest("other-user", "device-1", "token-1"), authentication));
    }

    @Test
    void removePushTokenUsesAuthenticatedUserAndDeviceId() {
        when(pushTokenRegistrationService.removePushToken("user-1", "device-1")).thenReturn(Mono.empty());

        var response = controller.removePushToken("device-1", authentication).block();

        assertEquals("Push token removed successfully", response.getMessage());
        verify(pushTokenRegistrationService).removePushToken("user-1", "device-1");
    }
}
