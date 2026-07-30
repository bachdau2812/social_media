package com.dauducbach.clone.modules.auth.controller;

import com.dauducbach.clone.modules.auth.dto.request.LoginRequest;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import com.dauducbach.clone.modules.auth.service.UserAccountQueryService;
import com.dauducbach.clone.modules.auth.service.AuthCookieService;
import com.dauducbach.clone.modules.auth.service.AuthenticationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AuthenticationControllerTest {
    AuthenticationService authenticationService;
    AuthCookieService authCookieService;
    UserAccountQueryService userAccountQueryService;
    AuthenticationController controller;
    WebTestClient client;

    @BeforeEach
    void setUp() {
        authenticationService = mock(AuthenticationService.class);
        authCookieService = mock(AuthCookieService.class);
        userAccountQueryService = mock(UserAccountQueryService.class);
        controller = new AuthenticationController(
                authenticationService,
                authCookieService,
                userAccountQueryService
        );

        client = WebTestClient.bindToController(
                        controller
                )
                .build();
    }

    @Test
    void loginReturnsCurrentUserForFrontendSession() {
        when(authenticationService.login(any(LoginRequest.class)))
                .thenReturn(Mono.just(AuthenticationResponse.builder()
                        .accessToken("access-token")
                        .refreshToken("refresh-token")
                        .deviceInfo("desktop")
                        .userId("user-42")
                        .username("bach")
                        .build()));

        client.post()
                .uri("/auth/login")
                .bodyValue(LoginRequest.builder()
                        .username("bach")
                        .password("secret")
                        .deviceInfo("desktop")
                        .build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.message").isEqualTo("Login successful")
                .jsonPath("$.result.userId").isEqualTo("user-42")
                .jsonPath("$.result.username").isEqualTo("bach")
                .jsonPath("$.result.message").isEqualTo("Login successful");
    }

    @Test
    void sessionReturnsIdentityFromAuthenticatedUser() {
        Authentication authentication = mock(Authentication.class);
        when(authentication.getName()).thenReturn("user-42");
        when(userAccountQueryService.getSessionIdentity("user-42"))
                .thenReturn(Mono.just(new UserAccountQueryService.SessionIdentity(
                        "user-42", "bach")));

        var exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/auth/session").build()
        );

        StepVerifier.create(controller.session(authentication, exchange))
                .assertNext(response -> {
                    org.junit.jupiter.api.Assertions.assertEquals("Session active", response.getMessage());
                    org.junit.jupiter.api.Assertions.assertEquals("user-42", response.getResult().getUserId());
                    org.junit.jupiter.api.Assertions.assertEquals("bach", response.getResult().getUsername());
                })
                .verifyComplete();
    }
}
