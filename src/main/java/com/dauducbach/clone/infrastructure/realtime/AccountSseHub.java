package com.dauducbach.clone.infrastructure.realtime;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;
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

/** Process-local SSE transport shared by modules that publish account-scoped updates. */
@Slf4j
@Service
public class AccountSseHub implements UserSsePublisher {
    private static final Duration KEEP_ALIVE = Duration.ofSeconds(15);
    static final int EVENT_BUFFER_CAPACITY = 256;

    private final Map<String, UserChannel> userChannels = new ConcurrentHashMap<>();

    public Flux<ServerSentEvent<String>> subscribe(String userId) {
        return Flux.defer(() -> {
            UserChannel channel = userChannels.computeIfAbsent(userId, ignored -> new UserChannel());
            channel.subscribers.incrementAndGet();
            return channel.sink.asFlux()
                    .mergeWith(Flux.interval(KEEP_ALIVE)
                            .map(sequence -> ServerSentEvent.builder(":keep-alive").build()))
                    .doFinally(signal -> removeSubscriber(userId, channel));
        });
    }

    @Override
    public Mono<Void> sendToUser(String userId, String event, String data) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        return Mono.fromRunnable(() -> sendToLocalUser(userId, event, data));
    }

    int channelCount() {
        return userChannels.size();
    }

    void sendToLocalUser(String userId, String event, String data) {
        UserChannel channel = userChannels.get(userId);
        if (channel == null) {
            return;
        }
        ServerSentEvent<String> message = ServerSentEvent.<String>builder()
                .event(event)
                .data(data)
                .build();
        Sinks.EmitResult result = channel.sink.tryEmitNext(message);
        if (result.isFailure()) {
            log.warn("|AccountSseHub|sendToLocalUser|emit failed|userId={}|event={}|result={}",
                    userId, event, result);
        }
    }

    private void removeSubscriber(String userId, UserChannel channel) {
        if (channel.subscribers.decrementAndGet() <= 0) {
            userChannels.remove(userId, channel);
        }
    }

    private static final class UserChannel {
        private final Sinks.Many<ServerSentEvent<String>> sink =
                Sinks.many().multicast().onBackpressureBuffer(EVENT_BUFFER_CAPACITY);
        private final AtomicInteger subscribers = new AtomicInteger();
    }
}
