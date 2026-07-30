package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.dto.response.ChatPresenceResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Service
public class ChatPresenceService {
    private static final Duration PRESENCE_TTL = Duration.ofSeconds(90);
    private static final Duration USER_SESSION_INDEX_TTL = Duration.ofSeconds(180);
    private static final String USER_SESSIONS_PREFIX = "chat:presence:user-sessions:";
    private static final String SESSION_PREFIX = "chat:presence:session:";
    private static final String LAST_ACTIVE_PREFIX = "chat:presence:last-active:";

    private final ReactiveRedisTemplate<String, Object> redisTemplate;
    private final String instanceId;

    public ChatPresenceService(
            ReactiveRedisTemplate<String, Object> redisTemplate,
            @Value("${app.instance-id:${random.uuid}}") String instanceId
    ) {
        this.redisTemplate = redisTemplate;
        this.instanceId = instanceId;
    }

    public Mono<Void> refresh(String userId, String sessionId) {
        Instant now = Instant.now();
        String sessionReference = sessionReference(sessionId);
        double expiresAt = now.plus(PRESENCE_TTL).toEpochMilli();
        return Mono.when(
                        redisTemplate.opsForZSet().add(
                                userSessionsKey(userId),
                                sessionReference,
                                expiresAt),
                        redisTemplate.expire(
                                userSessionsKey(userId),
                                USER_SESSION_INDEX_TTL),
                        redisTemplate.opsForValue().set(
                                sessionKey(userId, sessionReference),
                                "online",
                                PRESENCE_TTL),
                        redisTemplate.opsForValue().set(
                                lastActiveKey(userId),
                                now.toString()))
                .then();
    }

    public Mono<Boolean> isOnline(String userId) {
        return redisTemplate.opsForZSet()
                .range(userSessionsKey(userId), Range.unbounded())
                .map(Object::toString)
                .concatMap(sessionReference -> redisTemplate.hasKey(
                        sessionKey(userId, sessionReference)))
                .any(Boolean.TRUE::equals)
                .defaultIfEmpty(false);
    }

    public Mono<ChatPresenceResponse> getPresence(String userId) {
        return isOnline(userId)
                .flatMap(online -> redisTemplate.opsForValue()
                        .get(lastActiveKey(userId))
                        .map(value -> new ChatPresenceResponse(
                                userId,
                                online,
                                parseInstant(value)))
                        .defaultIfEmpty(new ChatPresenceResponse(
                                userId,
                                online,
                                null)));
    }

    public Mono<Void> remove(String userId, String sessionId) {
        String sessionReference = sessionReference(sessionId);
        return Mono.when(
                        redisTemplate.delete(sessionKey(userId, sessionReference)),
                        redisTemplate.opsForZSet().remove(
                                userSessionsKey(userId),
                                sessionReference))
                .then(isOnline(userId))
                .flatMap(online -> Boolean.TRUE.equals(online)
                        ? Mono.empty()
                        : redisTemplate.opsForValue()
                                .set(lastActiveKey(userId), Instant.now().toString())
                                .then());
    }

    private String sessionReference(String sessionId) {
        return instanceId + ":" + sessionId;
    }

    private String userSessionsKey(String userId) {
        return USER_SESSIONS_PREFIX + userId;
    }

    private String sessionKey(String userId, String sessionReference) {
        return SESSION_PREFIX + userId + ":" + sessionReference;
    }

    private String lastActiveKey(String userId) {
        return LAST_ACTIVE_PREFIX + userId;
    }

    private Instant parseInstant(Object value) {
        try {
            return value == null ? null : Instant.parse(value.toString());
        } catch (RuntimeException ignored) {
            return null;
        }
    }
}