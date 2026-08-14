package com.dauducbach.clone.modules.media.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class MusicFetchRequestLoggingFilter implements WebFilter {
    private static final Logger log = LoggerFactory.getLogger(MusicFetchRequestLoggingFilter.class);
    private static final Pattern FETCH_PATH = Pattern.compile(
            "(?:^|/)musics/([A-Za-z0-9]{22})/fetch/?$");

    @Override
    public @NonNull Mono<Void> filter(
            @NonNull ServerWebExchange exchange,
            @NonNull WebFilterChain chain) {
        Optional<String> trackId = musicTrackId(exchange.getRequest());
        if (trackId.isEmpty()) {
            return chain.filter(exchange);
        }

        long startedAt = System.nanoTime();
        String cleanTrackId = trackId.orElseThrow();
        log.info(
                "|MusicFetchRequest|received|method=POST|trackId={}|originPresent={}",
                cleanTrackId,
                exchange.getRequest().getHeaders().getOrigin() != null);

        return chain.filter(exchange)
                .doOnError(error -> log.error(
                        "|MusicFetchRequest|failed|trackId={}|errorType={}",
                        cleanTrackId,
                        error.getClass().getSimpleName()))
                .doFinally(signal -> log.info(
                        "|MusicFetchRequest|completed|trackId={}|httpStatus={}|signal={}|elapsedMs={}",
                        cleanTrackId,
                        responseStatus(exchange),
                        signal,
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt)));
    }

    static Optional<String> musicTrackId(ServerHttpRequest request) {
        if (request.getMethod() != HttpMethod.POST) {
            return Optional.empty();
        }
        Matcher matcher = FETCH_PATH.matcher(request.getURI().getPath());
        return matcher.find() ? Optional.of(matcher.group(1)) : Optional.empty();
    }

    private int responseStatus(ServerWebExchange exchange) {
        return exchange.getResponse().getStatusCode() == null
                ? HttpStatus.OK.value()
                : exchange.getResponse().getStatusCode().value();
    }
}