package com.dauducbach.clone.modules.post.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
public class PostSseService {
    private static final Duration KEEP_ALIVE = Duration.ofSeconds(15);
    private final Map<String, Sinks.Many<ServerSentEvent<String>>> userSinks = new ConcurrentHashMap<>();

    public Flux<ServerSentEvent<String>> subscribe(String userId) {
        Sinks.Many<ServerSentEvent<String>> sink = userSinks.computeIfAbsent(userId, key ->
                Sinks.many().multicast().onBackpressureBuffer()
        );

        return sink.asFlux()
                .mergeWith(Flux.interval(KEEP_ALIVE)
                        .map(seq -> ServerSentEvent.builder(":keep-alive").build()))
                .doOnCancel(() -> removeSink(userId));
    }

    public void sendToUser(String userId, String event, String data) {
        Sinks.Many<ServerSentEvent<String>> sink = userSinks.get(userId);
        if (sink == null) {
            log.warn("|PostSseService|sendToUser|no SSE subscriber for userId={}", userId);
            return;
        }

        ServerSentEvent<String> message = ServerSentEvent.<String>builder()
                .event(event)
                .data(data)
                .build();

        Sinks.EmitResult result = sink.tryEmitNext(message);
        if (result.isFailure()) {
            log.warn("|PostSseService|sendToUser|emit failed|userId={}|result={}", userId, result);
        } else if (result.isSuccess()) {
            log.info("|PostSseService|sendToUser|emit success|userId={}|result={}", userId, result);
        }
    }

    private void removeSink(String userId) {
        userSinks.remove(userId);
        log.info("|PostSseService|removeSink|removed SSE sink|userId={}", userId);
    }
}

