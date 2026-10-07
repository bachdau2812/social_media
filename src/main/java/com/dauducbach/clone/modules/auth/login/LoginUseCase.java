package com.dauducbach.clone.modules.auth.login;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.auth.credentials.CredentialAccount;
import com.dauducbach.clone.modules.auth.credentials.CredentialStore;
import com.dauducbach.clone.modules.auth.credentials.PasswordVerifier;
import com.dauducbach.clone.modules.auth.sessions.AuthTokenProvider;
import com.dauducbach.clone.modules.auth.sessions.AuthenticatedSession;
import com.dauducbach.clone.modules.auth.sessions.RefreshSession;
import com.dauducbach.clone.modules.auth.sessions.RefreshTokenGenerator;
import com.dauducbach.clone.modules.auth.sessions.RefreshSessionStore;
import com.google.gson.JsonObject;
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
public class LoginUseCase {
    private static final Logger log = LoggerFactory.getLogger(LoginUseCase.class);

    private final CredentialStore credentialStore;
    private final PasswordVerifier passwordVerifier;
    private final AuthTokenProvider tokenProvider;
    private final RefreshSessionStore refreshSessionStore;
    private final AuditRecorder auditRecorder;

    public Mono<AuthenticatedSession> login(String username, String password, String deviceInfo) {
        return credentialStore.findByUsername(username)
                .switchIfEmpty(Mono.defer(() -> saveAudit(
                        username,
                        username,
                        deviceInfo,
                        "FAILURE",
                        "USER_NOT_FOUND")
                        .then(Mono.error(new AppException(ErrorCode.USER_NOT_FOUND)))))
                .flatMap(account -> validatePassword(account, username, password, deviceInfo));
    }

    private Mono<AuthenticatedSession> validatePassword(
            CredentialAccount account,
            String username,
            String password,
            String deviceInfo) {
        if (!passwordVerifier.matches(password, account.passwordHash())) {
            return saveAudit(account.userId(), username, deviceInfo, "FAILURE", "PASSWORD_INCORRECT")
                    .then(Mono.error(new AppException(ErrorCode.PASSWORD_INCORRECT)));
        }

        return tokenProvider.issueAccessToken(account.userId(), account.role())
                .flatMap(accessToken -> {
                    String refreshToken = RefreshTokenGenerator.generateRawToken();
                    Instant now = Instant.now();
                    RefreshSession session = new RefreshSession(
                            UUID.randomUUID().toString(),
                            account.userId(),
                            RefreshTokenGenerator.sha256(refreshToken),
                            now.plus(5, ChronoUnit.DAYS),
                            now,
                            deviceInfo);
                    AuthenticatedSession result = new AuthenticatedSession(
                            accessToken,
                            refreshToken,
                            deviceInfo,
                            account.userId(),
                            account.username());

                    return refreshSessionStore.save(session)
                            .doOnSuccess(ignored -> log.info(
                                    "|LoginUseCase|login|refresh session saved|userId={}|sessionId={}",
                                    account.userId(),
                                    session.id()))
                            .onErrorMap(error -> {
                                log.error("|LoginUseCase|login|refresh session save failed|userId={}|errorType={}",
                                        account.userId(), error.getClass().getSimpleName());
                                return new AppException(ErrorCode.AUTHENTICATION_FAILED);
                            })
                            .then(saveAudit(account.userId(), username, deviceInfo, "SUCCESS", null))
                            .thenReturn(result);
                });
    }

    private Mono<Void> saveAudit(
            String actorId,
            String username,
            String deviceInfo,
            String status,
            String reason) {
        JsonObject metadata = new JsonObject();
        if (username != null && !username.isBlank()) metadata.addProperty("username", username);
        if (deviceInfo != null && !deviceInfo.isBlank()) metadata.addProperty("deviceInfo", deviceInfo);
        if (reason != null && !reason.isBlank()) metadata.addProperty("reason", reason);
        return auditRecorder.record(new AuditEntry(
                actorId,
                AuditActionType.LOGIN,
                "AUTH_SESSION",
                actorId,
                status,
                metadata.toString(),
                null));
    }
}
