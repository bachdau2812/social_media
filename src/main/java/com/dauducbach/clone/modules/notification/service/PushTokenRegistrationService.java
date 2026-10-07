package com.dauducbach.clone.modules.notification.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.notification.dto.request.PushTokenRegisterRequest;
import com.dauducbach.clone.modules.notification.dto.response.PushTokenRegisterResponse;
import com.dauducbach.clone.modules.notification.entity.NotificationPushToken;
import com.dauducbach.clone.modules.notification.repository.UserPushNotificationRepository;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PushTokenRegistrationService {
    private static final Logger log = LoggerFactory.getLogger(PushTokenRegistrationService.class);

    private final UserPushNotificationRepository tokenRepository;

    public Mono<PushTokenRegisterResponse> registerPushToken(PushTokenRegisterRequest request) {
        return Mono.defer(() -> {
            validate(request);
            String userId = request.userId().trim();
            String deviceId = normalizeOptional(request.deviceId());
            String deviceToken = request.deviceToken().trim();

            return tokenRepository.findByUserId(userId)
                    .flatMap(existing -> update(existing, deviceId, deviceToken))
                    .switchIfEmpty(Mono.defer(() -> create(userId, deviceId, deviceToken)))
                    .map(this::toResponse)
                    .doOnSuccess(response -> log.info(
                            "|PushTokenRegistrationService|register|success|userId={}|tokenId={}",
                            response.userId(), response.id()))
                    .onErrorMap(error -> error instanceof AppException
                            ? error
                            : new AppException(
                                    ErrorCode.PUSH_TOKEN_SAVE_FAILED,
                                    "Save push token failed for userId=" + userId,
                                    error));
        });
    }

    public Mono<Void> removePushToken(String userId, String deviceId) {
        if (userId == null || userId.isBlank() || deviceId == null || deviceId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.PUSH_TOKEN_INVALID, "userId and deviceId are required"));
        }
        return tokenRepository.deleteByUserIdAndDeviceId(userId.trim(), deviceId.trim());
    }

    private void validate(PushTokenRegisterRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.PUSH_TOKEN_INVALID, "Request is required");
        }
        if (request.userId() == null || request.userId().isBlank()) {
            throw new AppException(ErrorCode.PUSH_TOKEN_INVALID, "userId is required");
        }
        if (request.deviceToken() == null || request.deviceToken().isBlank()) {
            throw new AppException(ErrorCode.PUSH_TOKEN_INVALID, "deviceToken is required");
        }
    }

    private Mono<NotificationPushToken> update(
            NotificationPushToken existing,
            String deviceId,
            String deviceToken
    ) {
        existing.setDeviceId(deviceId);
        existing.setDeviceToken(deviceToken);
        return tokenRepository.save(existing)
                .doOnSuccess(saved -> log.info(
                        "|PushTokenRegistrationService|update|success|userId={}|tokenId={}",
                        saved.getUserId(), saved.getId()));
    }

    private Mono<NotificationPushToken> create(String userId, String deviceId, String deviceToken) {
        NotificationPushToken token = NotificationPushToken.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .deviceId(deviceId)
                .deviceToken(deviceToken)
                .createdAt(Instant.now())
                .build();
        return tokenRepository.save(token)
                .doOnSuccess(saved -> log.info(
                        "|PushTokenRegistrationService|create|success|userId={}|tokenId={}",
                        saved.getUserId(), saved.getId()));
    }

    private PushTokenRegisterResponse toResponse(NotificationPushToken token) {
        return new PushTokenRegisterResponse(token.getId(), token.getUserId(), token.getDeviceId(), token.getCreatedAt());
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
