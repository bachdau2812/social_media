package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.dto.request.LogoutRequest;
import com.dauducbach.clone.modules.auth.dto.request.RefreshTokenRequest;
import com.dauducbach.clone.modules.auth.dto.response.AuthenticationResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseCookie;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

@Service
public class AuthCookieService {
    public static final String ACCESS_TOKEN_COOKIE = "accessToken";
    public static final String REFRESH_TOKEN_COOKIE = "refreshToken";
    public static final String DEVICE_INFO_COOKIE = "deviceInfo";

    @Value("${jwt.valid-duration}")
    private long accessTokenValidDuration;

    @Value("${auth.cookie.secure:false}")
    private boolean secure;

    @Value("${auth.cookie.same-site:Lax}")
    private String sameSite;

    private static final Duration REFRESH_TOKEN_DURATION = Duration.ofDays(5);
    private static final String COOKIE_PATH = "/";
    public void writeAuthCookies(ServerHttpResponse response, AuthenticationResponse authenticationResponse) {
        addCookie(response, ACCESS_TOKEN_COOKIE, authenticationResponse.getAccessToken(), Duration.ofSeconds(accessTokenValidDuration));
        addCookie(response, REFRESH_TOKEN_COOKIE, authenticationResponse.getRefreshToken(), REFRESH_TOKEN_DURATION);

        if (StringUtils.hasText(authenticationResponse.getDeviceInfo())) {
            addCookie(response, DEVICE_INFO_COOKIE, authenticationResponse.getDeviceInfo(), REFRESH_TOKEN_DURATION);
        }
    }

    public void clearAuthCookies(ServerHttpResponse response) {
        clearCookie(response, ACCESS_TOKEN_COOKIE);
        clearCookie(response, REFRESH_TOKEN_COOKIE);
        clearCookie(response, DEVICE_INFO_COOKIE);
    }

    public String getCookieValue(ServerWebExchange exchange, String cookieName) {
        var cookie = exchange.getRequest().getCookies().getFirst(cookieName);
        if (cookie == null || !StringUtils.hasText(cookie.getValue())) {
            return null;
        }

        return decodeCookieValue(cookie.getValue());
    }

    private void addCookie(ServerHttpResponse response, String name, String value, Duration maxAge) {
        if (!StringUtils.hasText(value)) {
            return;
        }

        response.addCookie(ResponseCookie.from(name, encodeCookieValue(value))
                .httpOnly(true)
                .secure(secure)
                .sameSite(sameSite)
                .path(COOKIE_PATH)
                .maxAge(maxAge)
                .build());
    }

    private String encodeCookieValue(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private String decodeCookieValue(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8);
        } catch (IllegalArgumentException e) {
            return value;
        }
    }
    private void clearCookie(ServerHttpResponse response, String name) {
        response.addCookie(ResponseCookie.from(name, "")
                .httpOnly(true)
                .secure(secure)
                .sameSite(sameSite)
                .path(COOKIE_PATH)
                .maxAge(Duration.ZERO)
                .build());
    }

    public RefreshTokenRequest resolveRefreshTokenRequest(ServerWebExchange exchange, RefreshTokenRequest request) {
        String refreshToken = request != null && StringUtils.hasText(request.getRefreshToken())
                ? request.getRefreshToken()
                : getCookieValue(exchange, REFRESH_TOKEN_COOKIE);

        String deviceInfo = request != null && StringUtils.hasText(request.getDeviceInfo())
                ? request.getDeviceInfo()
                : getCookieValue(exchange, DEVICE_INFO_COOKIE);

        return RefreshTokenRequest.builder()
                .refreshToken(refreshToken)
                .deviceInfo(deviceInfo)
                .build();
    }

    public LogoutRequest resolveLogoutRequest(ServerWebExchange exchange, LogoutRequest request) {
        String accessToken = request != null && StringUtils.hasText(request.getAccessToken())
                ? request.getAccessToken()
                : getCookieValue(exchange, ACCESS_TOKEN_COOKIE);

        String refreshToken = request != null && StringUtils.hasText(request.getRefreshToken())
                ? request.getRefreshToken()
                : getCookieValue(exchange, REFRESH_TOKEN_COOKIE);

        String deviceInfo = request != null && StringUtils.hasText(request.getDeviceInfo())
                ? request.getDeviceInfo()
                : getCookieValue(exchange, DEVICE_INFO_COOKIE);

        return LogoutRequest.builder()
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .deviceInfo(deviceInfo)
                .build();
    }
}

