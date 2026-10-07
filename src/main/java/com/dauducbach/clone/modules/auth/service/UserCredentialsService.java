package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.dto.request.CreateUserRequest;
import com.dauducbach.clone.modules.auth.dto.request.EmailVerifyRequest;
import com.dauducbach.clone.modules.auth.recovery.CredentialRecoveryUseCase;
import com.dauducbach.clone.modules.auth.registration.RegistrationDraft;
import com.dauducbach.clone.modules.auth.registration.RegistrationUseCase;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Compatibility facade for existing registration and credential recovery controllers. */
@Service
@RequiredArgsConstructor
public class UserCredentialsService {
    private final RegistrationUseCase registrationUseCase;
    private final CredentialRecoveryUseCase credentialRecoveryUseCase;

    public Mono<Void> preRegister(CreateUserRequest request) {
        return registrationUseCase.preRegister(toDraft(request));
    }

    public Mono<String> emailVerifyAndCreateUser(EmailVerifyRequest request) {
        return registrationUseCase.verifyAndCreate(request.getEmail(), request.getCode());
    }

    public Mono<String> checkAndSendCodeForForgetPassword(String email) {
        return credentialRecoveryUseCase.requestPasswordReset(email);
    }

    public Mono<String> verifyAndSendNewPasswordToUser(EmailVerifyRequest request) {
        return credentialRecoveryUseCase.resetPassword(request.getEmail(), request.getCode());
    }

    public Mono<String> verifyAndSendNewUserNameAndNewPasswordToUser(EmailVerifyRequest request) {
        return credentialRecoveryUseCase.resetUsernameAndPassword(request.getEmail(), request.getCode());
    }

    private RegistrationDraft toDraft(CreateUserRequest request) {
        return new RegistrationDraft(
                request.getFullName(),
                request.getUsername(),
                request.getPassword(),
                request.getEmail(),
                request.getPhoneNumber(),
                request.getDob(),
                request.getSex(),
                request.getLivingIn(),
                request.getHometown(),
                request.getHobbyList(),
                request.getRole() == null ? "USER" : request.getRole());
    }
}
