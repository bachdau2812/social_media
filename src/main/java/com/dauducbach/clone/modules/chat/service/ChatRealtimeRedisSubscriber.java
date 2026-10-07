package com.dauducbach.clone.modules.chat.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

@Component
@RequiredArgsConstructor
public class ChatRealtimeRedisSubscriber {
    private static final Logger log = LoggerFactory.getLogger(ChatRealtimeRedisSubscriber.class);

    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final ChatRealtimeLocalDispatcher localDispatcher;
    private final ChatSessionRegistry sessionRegistry;
    private final AtomicBoolean recovering = new AtomicBoolean();
    private Disposable subscription;

    @PostConstruct
    void subscribe() {
        RedisSerializationContext.SerializationPair<String> stringPair =
                RedisSerializationContext.SerializationPair.fromSerializer(
                        new StringRedisSerializer());
        subscription = listenerContainer.receive(
                        java.util.List.of(ChannelTopic.of(ChatRealtimeFanoutPublisher.CHANNEL)),
                        stringPair,
                        stringPair)
                .doOnSubscribe(ignored -> {
                    if (recovering.compareAndSet(true, false)) {
                        // Sessions opened while Redis was unavailable also need a sync after recovery.
                        sessionRegistry.closeAll(null);
                    }
                })
                .map(ReactiveSubscription.Message::getMessage)
                .concatMap(localDispatcher::dispatch)
                .doOnError(error -> {
                    recovering.set(true);
                    sessionRegistry.closeAll(error);
                    log.error("|ChatRealtimeRedisSubscriber|subscribe|failed|error={}", error.getMessage());
                })
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(250)).maxBackoff(Duration.ofSeconds(30)))
                .subscribe();
    }

    @PreDestroy
    void dispose() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}
