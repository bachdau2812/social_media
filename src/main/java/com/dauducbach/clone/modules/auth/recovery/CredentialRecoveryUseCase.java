package com.dauducbach.clone.modules.auth.recovery;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.auth.credentials.AuthAccountCommands;
import com.dauducbach.clone.modules.auth.credentials.TemporarySecretGenerator;
import com.dauducbach.clone.modules.auth.notifications.AuthNotificationPublisher;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class CredentialRecoveryUseCase {
    private static final Duration CODE_TTL = Duration.ofMinutes(2);

    private final AuthAccountCommands accountCommands;
    private final RecoveryChallengeStore challengeStore;
    private final AuthNotificationPublisher notificationPublisher;
    private final TemporarySecretGenerator secretGenerator;
    private final AuditRecorder auditRecorder;

    @Transactional
    public Mono<String> requestPasswordReset(String email) {
        Mono<String> operation = accountCommands.emailExists(email)
                .flatMap(exists -> {
                    if (!exists) {
                        return Mono.error(new AppException(ErrorCode.EMAIL_NOT_LINKED));
                    }
                    String code = secretGenerator.verificationCode(10);
                    return notificationPublisher.sendRecoveryCode(email, code)
                            .then(challengeStore.saveRecoveryCode(email, code, CODE_TTL));
                })
                .thenReturn("Check email and send code success");
        return recordFailure(email, AuditActionType.FORGET_PASSWORD, operation);
    }

    @Transactional
    public Mono<String> resetPassword(String email, String submittedCode) {
        Mono<String> operation = challengeStore.findRecoveryCode(email)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.TIMEOUT)))
                .flatMap(expectedCode -> {
                    if (!expectedCode.equals(submittedCode)) {
                        return Mono.error(new AppException(ErrorCode.INVALID_VERIFICATION_CODE));
                    }
                    String newPassword = secretGenerator.password(12);
                    return accountCommands.findByEmail(email)
                            .flatMap(account -> notificationPublisher.sendNewPassword(email, newPassword)
                                    .then(accountCommands.updatePassword(account.userId(), newPassword))
                                    .thenReturn("New password was set and sent to your email successfully"));
                });
        return recordFailure(email, AuditActionType.RESET_PASSWORD, operation);
    }

    @Transactional
    public Mono<String> resetUsernameAndPassword(String email, String submittedCode) {
        Mono<String> operation = challengeStore.findRecoveryCode(email)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.TIMEOUT)))
                .flatMap(expectedCode -> {
                    if (!expectedCode.equals(submittedCode)) {
                        return Mono.error(new AppException(ErrorCode.INVALID_VERIFICATION_CODE));
                    }
                    String newPassword = secretGenerator.password(12);
                    return accountCommands.findByEmail(email)
                            .flatMap(account -> notificationPublisher.sendNewUsernameAndPassword(email, newPassword)
                                    .then(accountCommands.updateUsernameAndPassword(account.userId(), email, newPassword))
                                    .thenReturn("New password was set and sent to your email successfully"));
                });
        return recordFailure(email, AuditActionType.RESET_PASSWORD, operation);
    }

    private Mono<String> recordFailure(String email, AuditActionType action, Mono<String> operation) {
        return operation.onErrorResume(error -> auditRecorder.record(new AuditEntry(
                        email,
                        "EMAIL",
                        action,
                        "PASSWORD",
                        email,
                        "FAILURE",
                        emailMetadata(email, error.getMessage()).toString(),
                        null))
                .then(Mono.error(error)));
    }

    private JsonObject emailMetadata(String email, String reason) {
        JsonObject metadata = new JsonObject();
        metadata.addProperty("email", email);
        if (reason != null && !reason.isBlank()) {
            metadata.addProperty("reason", reason);
        }
        return metadata;
    }
}
