package com.dauducbach.clone.configuration;

import com.dauducbach.clone.modules.auth.dto.request.RefreshTokenRequest;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import com.dauducbach.clone.modules.auth.service.AuthCookieService;
import com.dauducbach.clone.modules.auth.service.AuthenticationService;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CookieServerAuthenticationConverterTest {
    AuthenticationService authenticationService = mock(AuthenticationService.class);
    AuthCookieService authCookieService = mock(AuthCookieService.class);
    CookieServerAuthenticationConverter converter = new CookieServerAuthenticationConverter(authenticationService, authCookieService);

    @Test
    void convertReturnsBearerTokenFromAccessTokenCookie() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/posts")
                .cookie(new HttpCookie(AuthCookieService.ACCESS_TOKEN_COOKIE, "access-token"))
                .build());

        StepVerifier.create(converter.convert(exchange))
                .assertNext(authentication -> {
                    assertThat(authentication).isInstanceOf(BearerTokenAuthenticationToken.class);
                    assertThat(((BearerTokenAuthenticationToken) authentication).getToken()).isEqualTo("access-token");
                })
                .verifyComplete();

        verify(authenticationService, never()).refreshToken(any());
    }

    @Test
    void convertRefreshesWhenAccessTokenIsMissingAndRefreshContextExists() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/posts")
                .cookie(new HttpCookie(AuthCookieService.REFRESH_TOKEN_COOKIE, "refresh-token"))
                .cookie(new HttpCookie(AuthCookieService.DEVICE_INFO_COOKIE, "desktop"))
                .build());
        RefreshTokenRequest refreshRequest = RefreshTokenRequest.builder()
                .refreshToken("refresh-token")
                .deviceInfo("desktop")
                .build();
        AuthenticationResponse refreshed = AuthenticationResponse.builder()
                .accessToken("new-access-token")
                .refreshToken("new-refresh-token")
                .deviceInfo("desktop")
                .build();

        when(authCookieService.resolveRefreshTokenRequest(exchange, null)).thenReturn(refreshRequest);
        when(authenticationService.refreshToken(refreshRequest)).thenReturn(Mono.just(refreshed));

        StepVerifier.create(converter.convert(exchange))
                .assertNext(authentication -> {
                    assertThat(authentication).isInstanceOf(BearerTokenAuthenticationToken.class);
                    assertThat(((BearerTokenAuthenticationToken) authentication).getToken()).isEqualTo("new-access-token");
                })
                .verifyComplete();

        verify(authCookieService).writeAuthCookies(exchange.getResponse(), refreshed);
    }

    @Test
    void convertReturnsEmptyWhenNoTokenContextExists() {
        MockServerWebExchange exchange = MockServerWebExchange.from(MockServerHttpRequest.get("/posts").build());
        when(authCookieService.resolveRefreshTokenRequest(exchange, null)).thenReturn(new RefreshTokenRequest());

        StepVerifier.create(converter.convert(exchange))
                .verifyComplete();

        verify(authenticationService, never()).refreshToken(any());
    }
}
