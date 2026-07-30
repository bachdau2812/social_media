package com.dauducbach.clone.modules.post.service;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import reactor.core.publisher.Mono;

@Component
@RequiredArgsConstructor
public class SseRealtimeRedisSubscriber {
    private static final Logger log = LoggerFactory.getLogger(SseRealtimeRedisSubscriber.class);

    private final ReactiveRedisMessageListenerContainer listenerContainer;
    private final ObjectMapper objectMapper;
    private final PostSseService postSseService;
    private Disposable subscription;

    @PostConstruct
    void subscribe() {
        RedisSerializationContext.SerializationPair<String> stringPair =
                RedisSerializationContext.SerializationPair.fromSerializer(
                        new StringRedisSerializer());
        subscription = listenerContainer.receive(
                        java.util.List.of(ChannelTopic.of(SseRealtimeFanoutPublisher.CHANNEL)),
                        stringPair,
                        stringPair)
                .map(ReactiveSubscription.Message::getMessage)
                .concatMap(this::dispatch)
                .doOnError(error -> log.error(
                        "|SseRealtimeRedisSubscriber|subscribe|failed|error={}",
                        error.getMessage()))
                .retry()
                .subscribe();
    }

    Mono<Void> dispatch(String payload) {
        return Mono.fromCallable(() -> objectMapper.readValue(
                        payload,
                        SseFanoutMessage.class))
                .doOnNext(message -> postSseService.sendToLocalUser(
                        message.userId(),
                        message.event(),
                        message.data()))
                .then();
    }

    @PreDestroy
    void dispose() {
        if (subscription != null) {
            subscription.dispose();
        }
    }
}