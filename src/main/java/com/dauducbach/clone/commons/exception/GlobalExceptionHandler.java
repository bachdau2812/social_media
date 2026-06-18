package com.dauducbach.clone.commons.exception;

import com.dauducbach.clone.commons.response.ApiResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

import java.util.Objects;

@RestControllerAdvice
public class GlobalExceptionHandler {
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(value = AppException.class)
    ResponseEntity<ApiResponse<?>> handleAppException(AppException appException, ServerWebExchange exchange) {
        ErrorCode errorCode = appException.getErrorCode();
        log.error("|GlobalExceptionHandler|handleAppException|code={}|message={}|cause={}",
                errorCode.getCode(),
                appException.getMessage(),
                appException.getCause() == null ? null : appException.getCause().getMessage());

        return ResponseEntity.status(errorCode.getHttpStatus()).body(
                ApiResponse.builder()
                        .code(errorCode.getCode())
                        .message(errorCode.getMessage())
                        .traceId(resolveTraceId(exchange))
                        .build()
        );
    }

    @ExceptionHandler(value = Exception.class)
    ResponseEntity<ApiResponse<?>> handleGlobalException(Exception exception, ServerWebExchange exchange) {
        log.error("|GlobalExceptionHandler|handleGlobalException|error={}", exception.getMessage());
        ApiResponse<Object> apiResponse = new ApiResponse<>();

        apiResponse.setCode(9999);
        apiResponse.setTraceId(resolveTraceId(exchange));
        apiResponse.setMessage("Unexpected error occurred");
        return ResponseEntity.badRequest().body(apiResponse);
    }

    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    ResponseEntity<ApiResponse<?>> handleValidateRequestBody(MethodArgumentNotValidException exception, ServerWebExchange exchange) {
        log.error("|GlobalExceptionHandler|handleValidateRequestBody|error={}", exception.getMessage());
        ApiResponse<Object> apiResponse = new ApiResponse<>();

        apiResponse.setCode(8888);
        apiResponse.setTraceId(resolveTraceId(exchange));
        apiResponse.setMessage(Objects.requireNonNull(exception.getFieldError()).getField() + ": " + Objects.requireNonNull(exception.getFieldError()).getDefaultMessage());
        return ResponseEntity.badRequest().body(apiResponse);
    }


    private String resolveTraceId(ServerWebExchange exchange) {
        Object traceId = exchange.getAttribute("traceId");
        if (traceId != null) {
            return traceId.toString();
        }

        return MDC.get("traceId");
    }


}
