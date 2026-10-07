package com.dauducbach.clone.modules.auth.sessions;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.auth.credentials.CredentialStore;
import com.google.gson.JsonObject;
import com.dauducbach.clone.modules.auth.sessions.RefreshTokenGenerator;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class RefreshSessionUseCase {
    private static final Logger log = LoggerFactory.getLogger(RefreshSessionUseCase.class);

    private final CredentialStore credentialStore;
    private final AuthTokenProvider tokenProvider;
    private final RefreshSessionStore refreshSessionStore;
    private final AuditRecorder auditRecorder;

    public Mono<AuthenticatedSession> refresh(String rawRefreshToken, String deviceInfo) {
        String tokenHash = RefreshTokenGenerator.sha256(rawRefreshToken);
        return refreshSessionStore.findCurrentValid(tokenHash, deviceInfo)
                .switchIfEmpty(Mono.defer(() -> saveAudit(
                        "UNKNOWN", deviceInfo, "FAILURE", "REFRESH_TOKEN_INVALID")
                        .then(Mono.error(new AppException(
                                ErrorCode.REFRESH_TOKEN_INVALID,
                                "Refresh token is invalid or expired. Please login again.")))))
                .flatMap(session -> credentialStore.findById(session.userId())
                        .doOnError(error -> log.error(
                                "|RefreshSessionUseCase|refresh|credential lookup failed|userId={}|errorType={}",
                                session.userId(), error.getClass().getSimpleName()))
                        .switchIfEmpty(Mono.defer(() -> saveAudit(
                                session.userId(), deviceInfo, "FAILURE", "USER_NOT_FOUND")
                                .then(Mono.error(new AppException(
                                        ErrorCode.USER_NOT_FOUND,
                                        "User not found. Please login again.")))))
                        .flatMap(account -> refreshSessionStore.revokeCurrent(tokenHash, deviceInfo)
                                .flatMap(revokedCount -> {
                                    if (revokedCount == 0) {
                                        return saveAudit(account.userId(), deviceInfo, "FAILURE", "REFRESH_TOKEN_REVOKED")
                                                .then(Mono.error(new AppException(
                                                        ErrorCode.REFRESH_TOKEN_INVALID,
                                                        "Refresh token is no longer valid. Please login again.")));
                                    }
                                    return issueReplacementSession(
                                            account.userId(),
                                            account.username(),
                                            account.role(),
                                            deviceInfo);
                                })
                                .onErrorResume(error -> revokeAndFail(account.userId(), error))));
    }

    private Mono<AuthenticatedSession> issueReplacementSession(
            String userId,
            String username,
            String role,
            String deviceInfo) {
        return tokenProvider.issueAccessToken(userId, role)
                .flatMap(accessToken -> {
                    String rawToken = RefreshTokenGenerator.generateRawToken();
                    Instant now = Instant.now();
                    RefreshSession session = new RefreshSession(
                            UUID.randomUUID().toString(),
                            userId,
                            RefreshTokenGenerator.sha256(rawToken),
                            now.plus(5, ChronoUnit.DAYS),
                            now,
                            deviceInfo);
                    AuthenticatedSession result = new AuthenticatedSession(
                            accessToken, rawToken, deviceInfo, userId, username);
                    return refreshSessionStore.save(session)
                            .doOnSuccess(ignored -> log.info(
                                    "|RefreshSessionUseCase|refresh|replacement saved|userId={}|sessionId={}",
                                    userId, session.id()))
                            .then(saveAudit(userId, deviceInfo, "SUCCESS", null))
                            .thenReturn(result);
                });
    }

    private Mono<AuthenticatedSession> revokeAndFail(String userId, Throwable error) {
        log.error("|RefreshSessionUseCase|refresh|failed|userId={}|errorType={}",
                userId, error.getClass().getSimpleName());
        return refreshSessionStore.revokeAllForUser(userId)
                .doOnSuccess(count -> log.info(
                        "|RefreshSessionUseCase|refresh|active sessions revoked|userId={}|count={}", userId, count))
                .doOnError(revokeError -> log.error(
                        "|RefreshSessionUseCase|refresh|session revocation failed|userId={}|errorType={}",
                        userId, revokeError.getClass().getSimpleName()))
                .onErrorResume(revokeError -> Mono.empty())
                .then(saveAudit(userId, null, "FAILURE", error.getClass().getSimpleName()))
                .then(Mono.error(new AppException(
                        ErrorCode.REFRESH_TOKEN_FAILED,
                        "Failed to refresh token. Your session has been revoked. Please login again.",
                        error)));
    }

    private Mono<Void> saveAudit(String actorId, String deviceInfo, String status, String reason) {
        JsonObject metadata = new JsonObject();
        if (deviceInfo != null && !deviceInfo.isBlank()) metadata.addProperty("deviceInfo", deviceInfo);
        if (reason != null && !reason.isBlank()) metadata.addProperty("reason", reason);
        return auditRecorder.record(new AuditEntry(
                actorId,
                AuditActionType.REFRESH_TOKEN,
                "AUTH_SESSION",
                actorId,
                status,
                metadata.toString(),
                null));
    }
}
