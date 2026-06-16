package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.entity.NotificationPushToken;
import com.dauducbach.clone.modules.notification.repository.NotificationEventsRepository;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PushNotificationServiceTest {
    @Mock
    UserPushNotificationRepository userPushNotificationRepository;
    @Mock
    NotificationEventsRepository notificationEventsRepository;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;

    @Test
    void registerPushTokenCreatesNewTokenWhenUserHasNoToken() {
        PushNotificationService service = newService();
        ReactiveInsertOperation.ReactiveInsert<NotificationPushToken> insertSpec = mock(ReactiveInsertOperation.ReactiveInsert.class);

        when(userPushNotificationRepository.findByUserId("user-1")).thenReturn(Mono.empty());
        when(r2dbcEntityTemplate.insert(NotificationPushToken.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(NotificationPushToken.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.registerPushToken(new PushTokenRegisterRequest("user-1", "device-1", "token-1")))
                .expectNextMatches(response -> response.id() != null
                        && response.userId().equals("user-1")
                        && response.deviceId().equals("device-1")
                        && response.createdAt() != null)
                .verifyComplete();
    }

    @Test
    void registerPushTokenUpdatesExistingTokenForUser() {
        PushNotificationService service = newService();
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
        PushNotificationService service = newService();

        StepVerifier.create(service.registerPushToken(new PushTokenRegisterRequest("user-1", "device-1", " ")))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.PUSH_TOKEN_INVALID)
                .verify();
    }

    private PushNotificationService newService() {
        return new PushNotificationService(userPushNotificationRepository, notificationEventsRepository, r2dbcEntityTemplate);
    }
}
