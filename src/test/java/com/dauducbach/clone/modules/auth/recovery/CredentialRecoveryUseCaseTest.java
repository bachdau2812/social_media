package com.dauducbach.clone.modules.auth.recovery;

import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.auth.credentials.AuthAccountCommands;
import com.dauducbach.clone.modules.auth.credentials.CredentialAccount;
import com.dauducbach.clone.modules.auth.credentials.TemporarySecretGenerator;
import com.dauducbach.clone.modules.auth.notifications.AuthNotificationPublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CredentialRecoveryUseCaseTest {
    @Mock AuthAccountCommands accountCommands;
    @Mock RecoveryChallengeStore challengeStore;
    @Mock AuthNotificationPublisher notificationPublisher;
    @Mock TemporarySecretGenerator secretGenerator;
    @Mock AuditRecorder auditRecorder;

    @Test
    void resetSendsTheGeneratedCredentialBeforePersistingItToPreserveCurrentFlow() {
        CredentialRecoveryUseCase useCase = new CredentialRecoveryUseCase(
                accountCommands, challengeStore, notificationPublisher, secretGenerator, auditRecorder);
        when(challengeStore.findRecoveryCode("alice@example.com")).thenReturn(Mono.just("code-123"));
        when(secretGenerator.password(12)).thenReturn("temporary-password");
        when(accountCommands.findByEmail("alice@example.com"))
                .thenReturn(Mono.just(new CredentialAccount("user-1", "alice", "hash", "USER")));
        when(notificationPublisher.sendNewPassword("alice@example.com", "temporary-password"))
                .thenReturn(Mono.empty());
        when(accountCommands.updatePassword("user-1", "temporary-password")).thenReturn(Mono.empty());

        StepVerifier.create(useCase.resetPassword("alice@example.com", "code-123"))
                .expectNext("New password was set and sent to your email successfully")
                .verifyComplete();

        InOrder inOrder = inOrder(notificationPublisher, accountCommands);
        inOrder.verify(notificationPublisher).sendNewPassword("alice@example.com", "temporary-password");
        inOrder.verify(accountCommands).updatePassword("user-1", "temporary-password");
    }

    @Test
    void missingRecoveryChallengeReturnsTimeoutWithoutChangingCredentials() {
        CredentialRecoveryUseCase useCase = new CredentialRecoveryUseCase(
                accountCommands, challengeStore, notificationPublisher, secretGenerator, auditRecorder);
        when(challengeStore.findRecoveryCode("alice@example.com")).thenReturn(Mono.empty());
        when(auditRecorder.record(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());

        StepVerifier.create(useCase.resetPassword("alice@example.com", "code"))
                .expectError()
                .verify();

        verify(accountCommands, org.mockito.Mockito.never())
                .findByEmail("alice@example.com");
    }
}
