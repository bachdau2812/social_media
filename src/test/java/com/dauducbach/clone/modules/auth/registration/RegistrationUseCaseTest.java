package com.dauducbach.clone.modules.auth.registration;

import com.dauducbach.clone.modules.auth.credentials.AuthAccountCommands;
import com.dauducbach.clone.modules.auth.credentials.TemporarySecretGenerator;
import com.dauducbach.clone.modules.auth.notifications.AuthNotificationPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RegistrationUseCaseTest {
    @Mock AuthAccountCommands accountCommands;
    @Mock RegistrationChallengeStore challengeStore;
    @Mock AuthNotificationPublisher notificationPublisher;
    @Mock TemporarySecretGenerator secretGenerator;

    @Test
    void preRegistrationStoresShortLivedDraftAndCodeBeforeSendingNotification() {
        RegistrationUseCase useCase = new RegistrationUseCase(
                accountCommands, challengeStore, notificationPublisher, secretGenerator);
        RegistrationDraft draft = new RegistrationDraft(
                "Alice", "alice", "secret", "alice@example.com", null, null,
                null, null, null, List.of(), "USER");
        when(accountCommands.usernameExists("alice")).thenReturn(Mono.just(false));
        when(accountCommands.emailExists("alice@example.com")).thenReturn(Mono.just(false));
        when(challengeStore.saveDraft("alice@example.com", draft, Duration.ofMinutes(5))).thenReturn(Mono.empty());
        when(secretGenerator.verificationCode(8)).thenReturn("code-123");
        when(challengeStore.saveRegistrationCode("alice@example.com", "code-123", Duration.ofMinutes(5)))
                .thenReturn(Mono.empty());
        when(notificationPublisher.sendRegistrationCode("alice@example.com", "alice", "code-123"))
                .thenReturn(Mono.empty());

        StepVerifier.create(useCase.preRegister(draft)).verifyComplete();

        verify(challengeStore).saveDraft("alice@example.com", draft, Duration.ofMinutes(5));
        verify(challengeStore).saveRegistrationCode("alice@example.com", "code-123", Duration.ofMinutes(5));
        verify(notificationPublisher).sendRegistrationCode("alice@example.com", "alice", "code-123");
    }

    @Test
    void verificationCreatesAccountAndPublishesProfileWithoutPuttingTransportInTheUseCase() {
        RegistrationUseCase useCase = new RegistrationUseCase(
                accountCommands, challengeStore, notificationPublisher, secretGenerator);
        RegistrationDraft draft = new RegistrationDraft(
                "Alice", "alice", "secret", "alice@example.com", null, null,
                null, null, null, List.of(), "USER");
        when(challengeStore.findDraft("alice@example.com")).thenReturn(Mono.just(draft));
        when(challengeStore.findRegistrationCode("alice@example.com")).thenReturn(Mono.just("12345678"));
        when(accountCommands.register(draft)).thenReturn(Mono.just("user-1"));
        when(notificationPublisher.publishProfileCreated(draft, "user-1")).thenReturn(Mono.empty());

        StepVerifier.create(useCase.verifyAndCreate("alice@example.com", "12345678"))
                .expectNext("Register success")
                .verifyComplete();

        verify(notificationPublisher).publishProfileCreated(draft, "user-1");
    }

    @Test
    void invalidVerificationCodeDoesNotCreateAnAccount() {
        RegistrationUseCase useCase = new RegistrationUseCase(
                accountCommands, challengeStore, notificationPublisher, secretGenerator);
        when(challengeStore.findDraft("alice@example.com")).thenReturn(Mono.just(new RegistrationDraft(
                "Alice", "alice", "secret", "alice@example.com", null, null,
                null, null, null, List.of(), "USER")));
        when(challengeStore.findRegistrationCode("alice@example.com")).thenReturn(Mono.just("expected"));

        StepVerifier.create(useCase.verifyAndCreate("alice@example.com", "wrong"))
                .expectError()
                .verify();

        verify(accountCommands, never()).register(any());
        verify(notificationPublisher, never()).publishProfileCreated(any(), any());
    }
}
