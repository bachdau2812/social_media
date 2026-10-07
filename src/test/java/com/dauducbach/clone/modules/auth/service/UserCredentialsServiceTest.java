package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.dto.request.CreateUserRequest;
import com.dauducbach.clone.modules.auth.dto.request.EmailVerifyRequest;
import com.dauducbach.clone.modules.auth.recovery.CredentialRecoveryUseCase;
import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import com.dauducbach.clone.modules.auth.registration.RegistrationUseCase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserCredentialsServiceTest {
    @Mock RegistrationUseCase registrationUseCase;
    @Mock CredentialRecoveryUseCase credentialRecoveryUseCase;

    @Test
    void registrationFacadeMapsHttpRequestToFeatureInput() {
        when(registrationUseCase.preRegister(any())).thenReturn(Mono.empty());
        UserCredentialsService service = new UserCredentialsService(registrationUseCase, credentialRecoveryUseCase);
        CreateUserRequest request = CreateUserRequest.builder()
                .fullName("Alice Example")
                .username("alice")
                .password("secret")
                .email("alice@example.com")
                .hobbyList(List.of("music"))
                .build();

        StepVerifier.create(service.preRegister(request)).verifyComplete();

        ArgumentCaptor<RegistrationDraft> draft = ArgumentCaptor.forClass(RegistrationDraft.class);
        verify(registrationUseCase).preRegister(draft.capture());
        assertThat(draft.getValue().username()).isEqualTo("alice");
        assertThat(draft.getValue().password()).isEqualTo("secret");
        assertThat(draft.getValue().role()).isEqualTo("USER");
        assertThat(draft.getValue().hobbyList()).containsExactly("music");
    }

    @Test
    void credentialRecoveryFacadePassesOnlyTheRequestedValues() {
        when(credentialRecoveryUseCase.resetPassword("alice@example.com", "654321"))
                .thenReturn(Mono.just("reset"));
        UserCredentialsService service = new UserCredentialsService(registrationUseCase, credentialRecoveryUseCase);

        StepVerifier.create(service.verifyAndSendNewPasswordToUser(EmailVerifyRequest.builder()
                        .email("alice@example.com")
                        .code("654321")
                        .build()))
                .expectNext("reset")
                .verifyComplete();

        verify(credentialRecoveryUseCase).resetPassword("alice@example.com", "654321");
    }
}
