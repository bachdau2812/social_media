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

@Component
@RequiredArgsConstructor
public class ChatRealtimeRedisSubscriber {
    private static final Logger log = LoggerFactory.getLogger(ChatRealtimeRedisSubscriber.class);

    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final ChatRealtimeLocalDispatcher localDispatcher;
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
                .map(ReactiveSubscription.Message::getMessage)
                .concatMap(localDispatcher::dispatch)
                .doOnError(error -> log.error(
                        "|ChatRealtimeRedisSubscriber|subscribe|failed|error={}",
                        error.getMessage()))
                .retry()
                .subscribe();
    }

    @PreDestroy
    void dispose() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}