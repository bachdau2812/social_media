package com.dauducbach.clone.configuration;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebExceptionHandler;

import reactor.core.publisher.Mono;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class FilterChainExceptionHandler implements WebExceptionHandler {
    private static final Logger logger = LoggerFactory.getLogger(FilterChainExceptionHandler.class);

    @Override
    public @NonNull Mono<Void> handle(@NonNull ServerWebExchange exchange, @NonNull Throwable ex) {
        logger.error("Reactive Filter Chain Exception:", ex);

        if (exchange.getResponse().isCommitted()) {
            return Mono.error(ex);
        }

        exchange.getResponse().setStatusCode(HttpStatus.INTERNAL_SERVER_ERROR);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_JSON);

        ApiResponse<Object> apiResponse = ApiResponse.builder()
                .code(9999)
                .message(ex.getMessage())
                .traceId(resolveTraceId(exchange))
                .build();

        try {
            byte[] bytes = new ObjectMapper().writeValueAsBytes(apiResponse);
            return exchange.getResponse().writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private String resolveTraceId(ServerWebExchange exchange) {
        Object traceId = exchange.getAttribute(TraceIdFilter.TRACE_ID_ATTR);
        return traceId == null ? null : traceId.toString();
    }
}