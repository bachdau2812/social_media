package com.dauducbach.clone.modules.auth.registration;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.credentials.AuthAccountCommands;
import com.dauducbach.clone.modules.auth.credentials.TemporarySecretGenerator;
import com.dauducbach.clone.modules.auth.notifications.AuthNotificationPublisher;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Service
@RequiredArgsConstructor
public class RegistrationUseCase {
    private static final Duration DRAFT_TTL = Duration.ofMinutes(5);
    private static final Duration CODE_TTL = Duration.ofMinutes(5);

    private final AuthAccountCommands accountCommands;
    private final RegistrationChallengeStore challengeStore;
    private final AuthNotificationPublisher notificationPublisher;
    private final TemporarySecretGenerator secretGenerator;

    public Mono<Void> preRegister(RegistrationDraft draft) {
        return Mono.zip(
                        accountCommands.usernameExists(draft.username()),
                        accountCommands.emailExists(draft.email()))
                .flatMap(existing -> {
                    if (existing.getT1()) {
                        return Mono.error(new AppException(ErrorCode.USERNAME_EXISTS));
                    }
                    if (existing.getT2()) {
                        return Mono.error(new RuntimeException("Email already exists"));
                    }
                    return challengeStore.saveDraft(draft.email(), draft, DRAFT_TTL)
                            .then(Mono.defer(() -> {
                                String code = secretGenerator.verificationCode(8);
                                return challengeStore.saveRegistrationCode(draft.email(), code, CODE_TTL)
                                        .then(notificationPublisher.sendRegistrationCode(
                                                draft.email(), draft.username(), code));
                            }));
                });
    }

    public Mono<String> verifyAndCreate(String email, String submittedCode) {
        return challengeStore.findDraft(email)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.INVALID_REGISTRATION_REQUEST_INFO)))
                .flatMap(draft -> challengeStore.findRegistrationCode(email)
                        .switchIfEmpty(Mono.error(new AppException(ErrorCode.INVALID_REGISTRATION_CODE_INFO)))
                        .flatMap(expectedCode -> {
                            if (!expectedCode.equals(submittedCode)) {
                                return Mono.error(new AppException(ErrorCode.INVALID_VERIFICATION_CODE));
                            }
                            return accountCommands.register(draft)
                                    .flatMap(userId -> notificationPublisher.publishProfileCreated(draft, userId)
                                            .thenReturn("Register success"));
                        }));
    }
}
