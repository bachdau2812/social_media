package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.configuration.TraceIdFilter;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.ApiResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.web.server.WebFilterExchange;
import org.springframework.security.web.server.authentication.ServerAuthenticationFailureHandler;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE,  makeFinal = true)

public class SocialLoginFailService implements ServerAuthenticationFailureHandler {
    AuthCookieService authCookieService;

    @Override
    public Mono<Void> onAuthenticationFailure(WebFilterExchange webFilterExchange, AuthenticationException exception) {
        var exchange = webFilterExchange.getExchange();
        var response = exchange.getResponse();

        authCookieService.clearAuthCookies(response);

        ErrorCode errorCode = resolveErrorCode(exception);
        ApiResponse<Object> apiResponse = ApiResponse.builder()
                .code(errorCode.getCode())
                .message(errorCode.getMessage())
                .traceId(resolveTraceId(exchange))
                .build();

        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        try {
            byte[] bytes = new ObjectMapper().writeValueAsBytes(apiResponse);
            return response.writeWith(Mono.just(response.bufferFactory().wrap(bytes)));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private ErrorCode resolveErrorCode(AuthenticationException exception) {
        if (exception == null || exception.getMessage() == null) {
            return ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL;
        }

        String message = exception.getMessage().toLowerCase();
        if (message.contains("missing") || message.contains("not found") || message.contains("null")) {
            return ErrorCode.MISSING_USER_INFO_FROM_SOCIAL_MEDIA;
        }

        return ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL;
    }

    private String resolveTraceId(org.springframework.web.server.ServerWebExchange exchange) {
        Object traceId = exchange.getAttribute(TraceIdFilter.TRACE_ID_ATTR);
        return traceId == null ? null : traceId.toString();
    }
}
