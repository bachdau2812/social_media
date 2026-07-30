package com.dauducbach.clone.modules.auth.controller;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.configuration.TraceIdFilter;
import com.dauducbach.clone.modules.auth.dto.request.IntrospectRequest;
import com.dauducbach.clone.modules.auth.dto.request.LoginRequest;
import com.dauducbach.clone.modules.auth.dto.request.LogoutRequest;
import com.dauducbach.clone.modules.auth.dto.request.RefreshTokenRequest;
import com.dauducbach.clone.modules.auth.dto.response.IntrospectResponse;
import com.dauducbach.clone.modules.auth.dto.response.LoginResponse;
import com.dauducbach.clone.modules.auth.service.AuthenticationService;
import com.dauducbach.clone.modules.auth.service.AuthCookieService;
import com.dauducbach.clone.modules.auth.service.UserAccountQueryService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/auth")
public class AuthenticationController {
    private static final Logger log = LoggerFactory.getLogger(AuthenticationController.class);
    private final AuthenticationService authenticationService;
    private final AuthCookieService authCookieService;
    private final UserAccountQueryService userAccountQueryService;

    @PostMapping("/login")
    public Mono<ApiResponse<LoginResponse>> login(@Valid @RequestBody LoginRequest request,
                                                  ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();

        return authenticationService.login(request)
                .map(authenticationResponse -> {
                    authCookieService.writeAuthCookies(response, authenticationResponse);
                    return ApiResponse.<LoginResponse>builder()
                            .message("Login successful")
                            .traceId(resolveTraceId(exchange))
                            .result(LoginResponse.builder()
                                    .userId(authenticationResponse.getUserId())
                                    .username(authenticationResponse.getUsername())
                                    .message("Login successful")
                                    .build())
                            .build();
                });
    }

    @GetMapping("/session")
    public Mono<ApiResponse<LoginResponse>> session(Authentication authentication,
                                                    ServerWebExchange exchange) {
        return userAccountQueryService.getSessionIdentity(authentication.getName())
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.USER_NOT_FOUND)))
                .map(userCredentials -> ApiResponse.<LoginResponse>builder()
                        .message("Session active")
                        .traceId(resolveTraceId(exchange))
                        .result(LoginResponse.builder()
                                .userId(userCredentials.userId())
                                .username(userCredentials.username())
                                .message("Session active")
                                .build())
                        .build());
    }

    @PostMapping("/refresh-token")
    public Mono<ApiResponse<String>> refreshToken(@RequestBody(required = false) RefreshTokenRequest request,
                                                  ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        RefreshTokenRequest resolvedRequest = authCookieService.resolveRefreshTokenRequest(exchange, request);

        if (!StringUtils.hasText(resolvedRequest.getRefreshToken()) || !StringUtils.hasText(resolvedRequest.getDeviceInfo())) {
            authCookieService.clearAuthCookies(response);
            return Mono.error(new AppException(ErrorCode.REFRESH_TOKEN_INVALID, "Refresh token or device info is missing. Please login again."));
        }

        return authenticationService.refreshToken(resolvedRequest)
                .map(authenticationResponse -> {
                    authCookieService.writeAuthCookies(response, authenticationResponse);
                    return ApiResponse.<String>builder()
                            .message("Refresh token successful")
                            .traceId(resolveTraceId(exchange))
                            .result("Refresh token successful")
                            .build();
                })
                .onErrorResume(ex -> {
                    authCookieService.clearAuthCookies(response);
                    return Mono.error(ex);
                });
    }

    @PostMapping("/logout")
    public Mono<ApiResponse<String>> logout(@RequestBody(required = false) LogoutRequest request,
                                            ServerWebExchange exchange) {
        ServerHttpResponse response = exchange.getResponse();
        LogoutRequest resolvedRequest = authCookieService.resolveLogoutRequest(exchange, request);

        if (!StringUtils.hasText(resolvedRequest.getAccessToken())
                || !StringUtils.hasText(resolvedRequest.getRefreshToken())
                || !StringUtils.hasText(resolvedRequest.getDeviceInfo())) {
            authCookieService.clearAuthCookies(response);
            return Mono.error(new AppException(ErrorCode.LOGOUT_FAILED, "Missing access token, refresh token or device info. Please login again."));
        }

        return authenticationService.logout(resolvedRequest)
                .map(message -> {
                    authCookieService.clearAuthCookies(response);
                    return ApiResponse.<String>builder()
                            .message(message)
                            .traceId(resolveTraceId(exchange))
                            .result(message)
                            .build();
                })
                .onErrorResume(ex -> {
                    authCookieService.clearAuthCookies(response);
                    return Mono.error(ex);
                });
    }

    @PostMapping("/introspect")
    public Mono<ApiResponse<IntrospectResponse>> introspect(@Valid @RequestBody IntrospectRequest request,
                                                            ServerWebExchange exchange) {
        return authenticationService.introspect(request)
                .map(result -> ApiResponse.<IntrospectResponse>builder()
                        .message("Introspect successful")
                        .traceId(resolveTraceId(exchange))
                        .result(result)
                        .build());
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object traceId = exchange.getAttribute(TraceIdFilter.TRACE_ID_ATTR);
        return traceId == null ? null : traceId.toString();
    }
}
