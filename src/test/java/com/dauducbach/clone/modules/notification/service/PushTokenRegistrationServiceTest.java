package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationPushToken;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PushTokenRegistrationServiceTest {
    @Mock
    UserPushNotificationRepository userPushNotificationRepository;

    @Test
    void registerPushTokenCreatesNewTokenWhenUserHasNoToken() {
        PushTokenRegistrationService service = newService();

        when(userPushNotificationRepository.findByUserId("user-1")).thenReturn(Mono.empty());
        when(userPushNotificationRepository.save(any(NotificationPushToken.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.registerPushToken(new PushTokenRegisterRequest("user-1", "device-1", "token-1")))
                .expectNextMatches(response -> response.id() != null
                        && response.userId().equals("user-1")
                        && response.deviceId().equals("device-1")
                        && response.createdAt() != null)
                .verifyComplete();
    }

    @Test
    void registerPushTokenUpdatesExistingTokenForUser() {
        PushTokenRegistrationService service = newService();
        NotificationPushToken existing = NotificationPushToken.builder()
                .id("token-id-1")
                .userId("user-1")
                .deviceId("old-device")
                .deviceToken("old-token")
                .createdAt(Instant.parse("2026-06-14T00:00:00Z"))
                .build();

        when(userPushNotificationRepository.findByUserId("user-1")).thenReturn(Mono.just(existing));
        when(userPushNotificationRepository.save(any(NotificationPushToken.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.registerPushToken(new PushTokenRegisterRequest("user-1", "device-2", "token-2")))
                .expectNextMatches(response -> response.id().equals("token-id-1")
                        && response.userId().equals("user-1")
                        && response.deviceId().equals("device-2")
                        && response.createdAt().equals(Instant.parse("2026-06-14T00:00:00Z")))
                .verifyComplete();

        verify(userPushNotificationRepository).save(existing);
    }

    @Test
    void registerPushTokenRejectsMissingDeviceToken() {
        PushTokenRegistrationService service = newService();

        StepVerifier.create(service.registerPushToken(new PushTokenRegisterRequest("user-1", "device-1", " ")))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.PUSH_TOKEN_INVALID)
                .verify();
    }

    @Test
    void removePushTokenDeletesOnlyTheAuthenticatedUsersDevice() {
        PushTokenRegistrationService service = newService();
        when(userPushNotificationRepository.deleteByUserIdAndDeviceId("user-1", "device-1"))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.removePushToken("user-1", "device-1"))
                .verifyComplete();

        verify(userPushNotificationRepository).deleteByUserIdAndDeviceId("user-1", "device-1");
    }

    private PushTokenRegistrationService newService() {
        return new PushTokenRegistrationService(userPushNotificationRepository);
    }
}
