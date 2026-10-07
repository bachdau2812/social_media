package com.dauducbach.clone.modules.auth.sessions;

import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.auth.credentials.CredentialAccount;
import com.dauducbach.clone.modules.auth.credentials.CredentialStore;
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
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AuthSessionUseCasesTest {
    @Mock CredentialStore credentialStore;
    @Mock AuthTokenProvider tokenProvider;
    @Mock RefreshSessionStore refreshSessionStore;
    @Mock AccessTokenRevocationStore accessTokenRevocationStore;
    @Mock AuditRecorder auditRecorder;

    @Test
    void refreshRotatesTheStoredTokenHashAndReturnsTheReplacement() {
        String oldToken = "old-refresh-token";
        when(refreshSessionStore.findCurrentValid(RefreshTokenGenerator.sha256(oldToken), "browser"))
                .thenReturn(Mono.just(new RefreshSession(
                        "session-1", "user-1", "old-hash", java.time.Instant.now().plusSeconds(300),
                        java.time.Instant.now(), "browser")));
        when(credentialStore.findById("user-1"))
                .thenReturn(Mono.just(new CredentialAccount("user-1", "alice", "hash", "USER")));
        when(refreshSessionStore.revokeCurrent(RefreshTokenGenerator.sha256(oldToken), "browser"))
                .thenReturn(Mono.just(1));
        when(tokenProvider.issueAccessToken("user-1", "USER")).thenReturn(Mono.just("new-access-token"));
        when(refreshSessionStore.save(any(RefreshSession.class))).thenReturn(Mono.empty());
        when(auditRecorder.record(any(AuditEntry.class))).thenReturn(Mono.empty());

        ArgumentCaptor<RefreshSession> saved = ArgumentCaptor.forClass(RefreshSession.class);
        AtomicReference<AuthenticatedSession> issued = new AtomicReference<>();
        StepVerifier.create(new RefreshSessionUseCase(
                        credentialStore, tokenProvider, refreshSessionStore, auditRecorder)
                        .refresh(oldToken, "browser"))
                .assertNext(issued::set)
                .verifyComplete();

        verify(refreshSessionStore).save(saved.capture());
        assertThat(issued.get().accessToken()).isEqualTo("new-access-token");
        assertThat(issued.get().refreshToken()).isNotBlank().isNotEqualTo(oldToken);
        assertThat(saved.getValue().tokenHash())
                .isEqualTo(RefreshTokenGenerator.sha256(issued.get().refreshToken()));
    }

    @Test
    void logoutRevokesBothTokenKindsAndRecordsTheOutcome() {
        String refreshToken = "refresh-token";
        when(refreshSessionStore.findCurrentValid(RefreshTokenGenerator.sha256(refreshToken), "browser"))
                .thenReturn(Mono.just(new RefreshSession(
                        "session-1", "user-1", "hash", java.time.Instant.now().plusSeconds(300),
                        java.time.Instant.now(), "browser")));
        when(accessTokenRevocationStore.revoke(any(), any())).thenReturn(Mono.just(true));
        when(refreshSessionStore.revokeCurrent(RefreshTokenGenerator.sha256(refreshToken), "browser"))
                .thenReturn(Mono.just(1));
        when(auditRecorder.record(any(AuditEntry.class))).thenReturn(Mono.empty());

        StepVerifier.create(new LogoutSessionUseCase(
                        refreshSessionStore, accessTokenRevocationStore, auditRecorder)
                        .logout("access-token", refreshToken, "browser"))
                .verifyComplete();

        verify(accessTokenRevocationStore).revoke(any(), any());
        verify(refreshSessionStore).revokeCurrent(RefreshTokenGenerator.sha256(refreshToken), "browser");
        verify(auditRecorder).record(any(AuditEntry.class));
    }

    @Test
    void introspectionMapsProviderFailureToInvalidToken() {
        when(tokenProvider.verifyAccessToken("bad-token")).thenReturn(Mono.error(new IllegalArgumentException()));

        StepVerifier.create(new IntrospectTokenUseCase(tokenProvider).isValid("bad-token"))
                .expectNext(false)
                .verifyComplete();
    }
}
