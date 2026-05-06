package com.dauducbach.clone.configuration;

import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;

import java.util.UUID;
import reactor.core.publisher.Mono;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TraceIdFilter implements WebFilter {
    public static final String TRACE_ID_ATTR = "traceId";
    public static final String TRACE_ID_HEADER = "X-Trace-Id";

    @Override
    public @NonNull Mono<Void> filter(@NonNull ServerWebExchange exchange, @NonNull WebFilterChain chain) {
        String traceId = resolveTraceId(exchange.getRequest());

        exchange.getAttributes().put(TRACE_ID_ATTR, traceId);
        exchange.getResponse().getHeaders().set(TRACE_ID_HEADER, traceId);
        MDC.put(TRACE_ID_ATTR, traceId);

        return chain.filter(exchange)
                .doFinally(signalType -> MDC.remove(TRACE_ID_ATTR));
    }

    private String resolveTraceId(ServerHttpRequest request) {
        String traceId = request.getHeaders().getFirst(TRACE_ID_HEADER);
        if (traceId == null || traceId.isBlank()) {
            return UUID.randomUUID().toString();
        }

        return traceId;
    }
}
