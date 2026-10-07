package com.dauducbach.clone.modules.auth.notifications;

import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import reactor.core.publisher.Mono;

public interface AuthNotificationPublisher {
    Mono<Void> sendRegistrationCode(String email, String username, String code);

    Mono<Void> publishProfileCreated(RegistrationDraft draft, String userId);

    Mono<Void> sendRecoveryCode(String email, String code);

    Mono<Void> sendNewPassword(String email, String newPassword);

    Mono<Void> sendNewUsernameAndPassword(String email, String newPassword);
}
