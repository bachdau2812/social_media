package com.dauducbach.clone.modules.notification.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Service
public class NotificationSseService {
    public static final String CHANGED_EVENT = "notification_changed";
    private static final Duration DEFAULT_KEEP_ALIVE = Duration.ofSeconds(15);

    private final Duration keepAlive;
    private final Map<String, UserChannel> channels = new ConcurrentHashMap<>();

    public NotificationSseService() {
        this(DEFAULT_KEEP_ALIVE);
    }

    NotificationSseService(Duration keepAlive) {
        this.keepAlive = keepAlive;
    }

    public Flux<ServerSentEvent<String>> subscribe(String userId) {
        return Flux.defer(() -> {
            UserChannel channel = channels.computeIfAbsent(userId, ignored -> new UserChannel());
            channel.subscribers.incrementAndGet();
            return channel.sink.asFlux()
                    .mergeWith(Flux.interval(keepAlive)
                            .map(sequence -> ServerSentEvent.<String>builder()
                                    .comment("keep-alive")
                                    .build()))
                    .doFinally(signal -> removeSubscriber(userId, channel));
        });
    }

    public Mono<Void> notifyChanged(String userId, String notificationId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> {
            UserChannel channel = channels.get(userId);
            if (channel == null) {
                return;
            }
            Sinks.EmitResult result = channel.sink.tryEmitNext(ServerSentEvent.<String>builder()
                    .event(CHANGED_EVENT)
                    .data(notificationId)
                    .build());
            if (result.isFailure()) {
                log.debug("|NotificationSseService|notifyChanged|emit skipped|userId={}|result={}", userId, result);
            }
        });
    }

    int channelCount() {
        return channels.size();
    }

    private void removeSubscriber(String userId, UserChannel channel) {
        if (channel.subscribers.decrementAndGet() <= 0) {
            channels.remove(userId, channel);
        }
    }

    private static final class UserChannel {
        private final Sinks.Many<ServerSentEvent<String>> sink =
                Sinks.many().multicast().onBackpressureBuffer();
        private final AtomicInteger subscribers = new AtomicInteger();
    }
}
