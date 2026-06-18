package com.dauducbach.clone.commons.exception;

import com.dauducbach.clone.commons.response.ApiResponse;
import org.junit.jupiter.api.Test;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;

import static org.assertj.core.api.Assertions.assertThat;

class GlobalExceptionHandlerTest {

    @Test
    void handleAppExceptionReturnsErrorCodeMessageInsteadOfDetailMessage() {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/test").build()
        );
        exchange.getAttributes().put("traceId", "trace-1");

        var response = handler.handleAppException(
                new AppException(ErrorCode.POST_FETCH_FAILED, "Fetch post failed for postId=post-1", new RuntimeException("sql timeout")),
                exchange
        );

        ApiResponse<?> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getCode()).isEqualTo(ErrorCode.POST_FETCH_FAILED.getCode());
        assertThat(body.getMessage()).isEqualTo(ErrorCode.POST_FETCH_FAILED.getMessage());
        assertThat(body.getMessage()).doesNotContain("post-1");
        assertThat(body.getTraceId()).isEqualTo("trace-1");
    }

    @Test
    void handleGlobalExceptionReturnsSafeMessage() {
        GlobalExceptionHandler handler = new GlobalExceptionHandler();
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/test").build()
        );

        var response = handler.handleGlobalException(new RuntimeException("connection secret failed"), exchange);

        ApiResponse<?> body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.getCode()).isEqualTo(9999);
        assertThat(body.getMessage()).isEqualTo("Unexpected error occurred");
        assertThat(body.getMessage()).doesNotContain("secret");
    }
}
