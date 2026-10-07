package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.dto.request.IntrospectRequest;
import com.dauducbach.clone.modules.auth.dto.request.LoginRequest;
import com.dauducbach.clone.modules.auth.dto.request.LogoutRequest;
import com.dauducbach.clone.modules.auth.dto.request.RefreshTokenRequest;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import com.dauducbach.clone.modules.auth.dto.response.IntrospectResponse;
import com.dauducbach.clone.modules.auth.login.LoginUseCase;
import com.dauducbach.clone.modules.auth.sessions.AuthenticatedSession;
import com.dauducbach.clone.modules.auth.sessions.IntrospectTokenUseCase;
import com.dauducbach.clone.modules.auth.sessions.LogoutSessionUseCase;
import com.dauducbach.clone.modules.auth.sessions.RefreshSessionUseCase;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Compatibility facade for the existing HTTP and security integration entry points. */
@Service
@RequiredArgsConstructor
public class AuthenticationService {
    private final LoginUseCase loginUseCase;
    private final RefreshSessionUseCase refreshSessionUseCase;
    private final LogoutSessionUseCase logoutSessionUseCase;
    private final IntrospectTokenUseCase introspectTokenUseCase;

    public Mono<AuthenticationResponse> login(LoginRequest request) {
        return loginUseCase.login(request.getUsername(), request.getPassword(), request.getDeviceInfo())
                .map(this::toLoginResponse);
    }

    public Mono<AuthenticationResponse> refreshToken(RefreshTokenRequest request) {
        return refreshSessionUseCase.refresh(request.getRefreshToken(), request.getDeviceInfo())
                .map(this::toRefreshResponse);
    }

    public Mono<String> logout(LogoutRequest request) {
        return logoutSessionUseCase.logout(
                        request.getAccessToken(),
                        request.getRefreshToken(),
                        request.getDeviceInfo())
                .thenReturn("Logout successful");
    }

    public Mono<IntrospectResponse> introspect(IntrospectRequest request) {
        return introspectTokenUseCase.isValid(request.getAccessToken())
                .map(valid -> IntrospectResponse.builder().valid(valid).build());
    }

    private AuthenticationResponse toLoginResponse(AuthenticatedSession session) {
        return AuthenticationResponse.builder()
                .accessToken(session.accessToken())
                .refreshToken(session.refreshToken())
                .deviceInfo(session.deviceInfo())
                .userId(session.userId())
                .username(session.username())
                .build();
    }

    private AuthenticationResponse toRefreshResponse(AuthenticatedSession session) {
        return AuthenticationResponse.builder()
                .accessToken(session.accessToken())
                .refreshToken(session.refreshToken())
                .deviceInfo(session.deviceInfo())
                .build();
    }
}
