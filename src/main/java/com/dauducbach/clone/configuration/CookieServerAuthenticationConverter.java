package com.dauducbach.clone.configuration;

import com.dauducbach.clone.modules.auth.dto.request.RefreshTokenRequest;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import com.dauducbach.clone.modules.auth.service.AuthCookieService;
import com.dauducbach.clone.modules.auth.service.AuthenticationService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpCookie;
import org.springframework.security.core.Authentication;
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthenticationToken;
import org.springframework.security.web.server.authentication.ServerAuthenticationConverter;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class CookieServerAuthenticationConverter implements ServerAuthenticationConverter {
    private static final Logger log = LoggerFactory.getLogger(CookieServerAuthenticationConverter.class);

    private final AuthenticationService authenticationService;
    private final AuthCookieService authCookieService;

    @Override
    public Mono<Authentication> convert(ServerWebExchange exchange) {
        String accessToken = getCookieValue(exchange, AuthCookieService.ACCESS_TOKEN_COOKIE);
        if (StringUtils.hasText(accessToken)) {
            log.info("|CookieServerAuthenticationConverter|convert|source=access_cookie");
            return Mono.just(new BearerTokenAuthenticationToken(accessToken));
        }

        RefreshTokenRequest refreshTokenRequest = authCookieService.resolveRefreshTokenRequest(exchange, null);
        if (!hasRefreshContext(refreshTokenRequest)) {
            log.info("|CookieServerAuthenticationConverter|convert|source=none|reason=missing_refresh_context");
            return Mono.empty();
        }

        log.info("|CookieServerAuthenticationConverter|convert|source=refresh_cookie|deviceInfo={}", refreshTokenRequest.getDeviceInfo());
        return authenticationService.refreshToken(refreshTokenRequest)
                .doOnNext(authenticationResponse -> authCookieService.writeAuthCookies(exchange.getResponse(), authenticationResponse))
                .map(AuthenticationResponse::getAccessToken)
                .filter(StringUtils::hasText)
                .map(BearerTokenAuthenticationToken::new)
                .cast(Authentication.class)
                .onErrorResume(error -> {
                    log.warn("|CookieServerAuthenticationConverter|convert|refresh_failed|deviceInfo={}|reason={}",
                            refreshTokenRequest.getDeviceInfo(),
                            error.getMessage());
                    authCookieService.clearAuthCookies(exchange.getResponse());
                    return Mono.empty();
                });
    }

    private String getCookieValue(ServerWebExchange exchange, String cookieName) {
        HttpCookie cookie = exchange.getRequest().getCookies().getFirst(cookieName);
        return cookie == null ? null : cookie.getValue();
    }

    private boolean hasRefreshContext(RefreshTokenRequest request) {
        return request != null
                && StringUtils.hasText(request.getRefreshToken())
                && StringUtils.hasText(request.getDeviceInfo());
    }
}
