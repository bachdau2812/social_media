package com.dauducbach.clone.modules.auth.login;

import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.auth.credentials.CredentialAccount;
import com.dauducbach.clone.modules.auth.credentials.CredentialStore;
import com.dauducbach.clone.modules.auth.credentials.PasswordVerifier;
import com.dauducbach.clone.modules.auth.sessions.RefreshTokenGenerator;
import com.dauducbach.clone.modules.auth.sessions.AuthTokenProvider;
import com.dauducbach.clone.modules.auth.sessions.RefreshSession;
import com.dauducbach.clone.modules.auth.sessions.RefreshSessionStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class LoginUseCaseTest {
    @Mock CredentialStore credentialStore;
    @Mock PasswordVerifier passwordVerifier;
    @Mock AuthTokenProvider tokenProvider;
    @Mock RefreshSessionStore refreshSessionStore;
    @Mock AuditRecorder auditRecorder;

    @Test
    void issuesAccessAndRefreshCredentialsAndStoresOnlyTheRefreshHash() {
        when(credentialStore.findByUsername("alice")).thenReturn(Mono.just(
                new CredentialAccount("user-1", "alice", "password-hash", "USER")));
        when(passwordVerifier.matches("plain-password", "password-hash")).thenReturn(true);
        when(tokenProvider.issueAccessToken("user-1", "USER")).thenReturn(Mono.just("access-token"));
        when(refreshSessionStore.save(any(RefreshSession.class))).thenReturn(Mono.empty());
        when(auditRecorder.record(any(AuditEntry.class))).thenReturn(Mono.empty());

        var useCase = new LoginUseCase(
                credentialStore, passwordVerifier, tokenProvider, refreshSessionStore, auditRecorder);

        ArgumentCaptor<RefreshSession> savedSession = ArgumentCaptor.forClass(RefreshSession.class);
        AtomicReference<String> issuedRefreshToken = new AtomicReference<>();
        StepVerifier.create(useCase.login("alice", "plain-password", "browser"))
                .assertNext(session -> {
                    assertThat(session.accessToken()).isEqualTo("access-token");
                    assertThat(session.refreshToken()).isNotBlank();
                    issuedRefreshToken.set(session.refreshToken());
                    assertThat(session.userId()).isEqualTo("user-1");
                    assertThat(session.deviceInfo()).isEqualTo("browser");
                })
                .verifyComplete();

        verify(refreshSessionStore).save(savedSession.capture());
        assertThat(savedSession.getValue().tokenHash())
                .isEqualTo(RefreshTokenGenerator.sha256(issuedRefreshToken.get()));
    }

    @Test
    void doesNotIssueOrPersistSessionWhenPasswordDoesNotMatch() {
        when(credentialStore.findByUsername("alice")).thenReturn(Mono.just(
                new CredentialAccount("user-1", "alice", "password-hash", "USER")));
        when(passwordVerifier.matches("wrong-password", "password-hash")).thenReturn(false);
        when(auditRecorder.record(any(AuditEntry.class))).thenReturn(Mono.empty());

        var useCase = new LoginUseCase(
                credentialStore, passwordVerifier, tokenProvider, refreshSessionStore, auditRecorder);

        StepVerifier.create(useCase.login("alice", "wrong-password", "browser"))
                .expectError()
                .verify();

        verifyNoInteractions(tokenProvider, refreshSessionStore);
    }
}
