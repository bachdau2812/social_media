package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.post.service.SseRealtimeFanoutPublisher;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class PostSseService implements UserSsePublisher {
    private static final Duration KEEP_ALIVE = Duration.ofSeconds(15);

    private final SseRealtimeFanoutPublisher fanoutPublisher;
    private final Map<String, UserChannel> userChannels = new ConcurrentHashMap<>();

    public Flux<ServerSentEvent<String>> subscribe(String userId) {
        UserChannel channel = userChannels.computeIfAbsent(
                userId,
                ignored -> new UserChannel());
        channel.subscribers.incrementAndGet();
        return channel.sink.asFlux()
                .mergeWith(Flux.interval(KEEP_ALIVE)
                        .map(sequence -> ServerSentEvent.builder(":keep-alive").build()))
                .doFinally(signal -> removeSubscriber(userId, channel));
    }

    @Override
    public Mono<Void> sendToUser(String userId, String event, String data) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        return fanoutPublisher.publish(userId, event, data)
                .onErrorResume(error -> {
                    log.warn(
                            "|PostSseService|sendToUser|redis fallback|userId={}|event={}|error={}",
                            userId, event, error.getMessage());
                    sendToLocalUser(userId, event, data);
                    return Mono.empty();
                });
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
            log.warn(
                    "|PostSseService|sendToLocalUser|emit failed|userId={}|event={}|result={}",
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
                Sinks.many().multicast().onBackpressureBuffer();
        private final AtomicInteger subscribers = new AtomicInteger();
    }
}
