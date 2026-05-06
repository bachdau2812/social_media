package com.dauducbach.clone.commons.exception;

import com.dauducbach.clone.commons.response.ApiResponse;
import org.slf4j.MDC;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ServerWebExchange;

import java.util.Objects;

@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(value = AppException.class)
    ResponseEntity<ApiResponse<?>> handleAppException(AppException appException, ServerWebExchange exchange) {
        ErrorCode errorCode = appException.getErrorCode();

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
        ApiResponse<Object> apiResponse = new ApiResponse<>();

        apiResponse.setCode(9999);
        apiResponse.setTraceId(resolveTraceId(exchange));
        apiResponse.setMessage(exception.getMessage());
        return ResponseEntity.badRequest().body(apiResponse);
    }

    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    ResponseEntity<ApiResponse<?>> handleValidateRequestBody(MethodArgumentNotValidException exception, ServerWebExchange exchange) {
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
