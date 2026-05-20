package com.dauducbach.clone.modules.auth.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.configuration.TraceIdFilter;
import com.dauducbach.clone.modules.auth.dto.request.CreateUserRequest;
import com.dauducbach.clone.modules.auth.dto.request.EmailVerifyRequest;
import com.dauducbach.clone.modules.auth.dto.request.SendCodeForForgetPasswordRequest;
import com.dauducbach.clone.modules.auth.service.UserCredentialsService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/auth/user-credentials")
public class UserCredentialsController {
    private final UserCredentialsService userCredentialsService;

    @PostMapping("/pre-register")
    public Mono<ApiResponse<String>> preRegister(@Valid @RequestBody CreateUserRequest request,
                                                 ServerWebExchange exchange) {
        return userCredentialsService.preRegister(request)
                .thenReturn(ApiResponse.<String>builder()
                        .message("Pre-register successful")
                        .traceId(resolveTraceId(exchange))
                        .result("Pre-register successful")
                        .build());
    }

    @PostMapping("/email-verify-and-create-user")
    public Mono<ApiResponse<String>> emailVerifyAndCreateUser(@Valid @RequestBody EmailVerifyRequest request,
                                                              ServerWebExchange exchange) {
        return userCredentialsService.emailVerifyAndCreateUser(request)
                .map(result -> ApiResponse.<String>builder()
                        .message(result)
                        .traceId(resolveTraceId(exchange))
                        .result(result)
                        .build());
    }

    @PostMapping("/check-and-send-code-for-forget-password")
    public Mono<ApiResponse<String>> checkAndSendCodeForForgetPassword(@Valid @RequestBody SendCodeForForgetPasswordRequest request,
                                                                        ServerWebExchange exchange) {
        return userCredentialsService.checkAndSendCodeForForgetPassword(request.getEmail())
                .map(result -> ApiResponse.<String>builder()
                        .message(result)
                        .traceId(resolveTraceId(exchange))
                        .result(result)
                        .build());
    }

    @PostMapping("/verify-and-send-new-password-to-user")
    public Mono<ApiResponse<String>> verifyAndSendNewPasswordToUser(@Valid @RequestBody EmailVerifyRequest request,
                                                                    ServerWebExchange exchange) {
        return userCredentialsService.verifyAndSendNewPasswordToUser(request)
                .map(result -> ApiResponse.<String>builder()
                        .message(result)
                        .traceId(resolveTraceId(exchange))
                        .result(result)
                        .build());
    }

    @PostMapping("/verify-and-send-new-username-and-new-password-to-user")
    public Mono<ApiResponse<String>> verifyAndSendNewUserNameAndNewPasswordToUser(@Valid @RequestBody EmailVerifyRequest request,
                                                                                  ServerWebExchange exchange) {
        return userCredentialsService.verifyAndSendNewUserNameAndNewPasswordToUser(request)
                .map(result -> ApiResponse.<String>builder()
                        .message(result)
                        .traceId(resolveTraceId(exchange))
                        .result(result)
                        .build());
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object traceId = exchange.getAttribute(TraceIdFilter.TRACE_ID_ATTR);
        return traceId == null ? null : traceId.toString();
    }
}

