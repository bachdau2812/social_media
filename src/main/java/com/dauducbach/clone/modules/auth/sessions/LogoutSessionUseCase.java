package com.dauducbach.clone.modules.auth.sessions;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.google.gson.JsonObject;
import com.dauducbach.clone.modules.auth.sessions.RefreshTokenGenerator;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class LogoutSessionUseCase {
    private static final Logger log = LoggerFactory.getLogger(LogoutSessionUseCase.class);
    private static final Duration ACCESS_TOKEN_REVOCATION_TTL = Duration.ofMinutes(15);

    private final RefreshSessionStore refreshSessionStore;
    private final AccessTokenRevocationStore accessTokenRevocationStore;
    private final AuditRecorder auditRecorder;

    public Mono<Void> logout(String accessToken, String refreshToken, String deviceInfo) {
        String tokenHash = RefreshTokenGenerator.sha256(refreshToken);
        return refreshSessionStore.findCurrentValid(tokenHash, deviceInfo)
                .map(RefreshSession::userId)
                .defaultIfEmpty("UNKNOWN")
                .flatMap(userId -> accessTokenRevocationStore.revoke(accessToken, ACCESS_TOKEN_REVOCATION_TTL)
                        .doOnError(error -> log.error(
                                "|LogoutSessionUseCase|logout|access token revocation failed|errorType={}",
                                error.getClass().getSimpleName()))
                        .onErrorMap(error -> new AppException(ErrorCode.LOGOUT_FAILED, "Logout failed", error))
                        .then(refreshSessionStore.revokeCurrent(tokenHash, deviceInfo))
                        .then(saveAudit(userId, deviceInfo, "SUCCESS", null))
                        .onErrorResume(error -> saveAudit(
                                        userId,
                                        deviceInfo,
                                        "FAILURE",
                                        error.getClass().getSimpleName())
                                .then(Mono.error(error))));
    }

    private Mono<Void> saveAudit(String actorId, String deviceInfo, String status, String reason) {
        JsonObject metadata = new JsonObject();
        if (deviceInfo != null && !deviceInfo.isBlank()) metadata.addProperty("deviceInfo", deviceInfo);
        if (reason != null && !reason.isBlank()) metadata.addProperty("reason", reason);
        return auditRecorder.record(new AuditEntry(
                actorId,
                AuditActionType.LOGOUT,
                "AUTH_SESSION",
                actorId,
                status,
                metadata.toString(),
                null));
    }
}
