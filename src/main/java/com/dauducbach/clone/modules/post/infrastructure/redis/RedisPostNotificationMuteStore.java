package com.dauducbach.clone.modules.post.infrastructure.redis;

import com.dauducbach.clone.modules.post.application.PostNotificationMuteStore;
import com.dauducbach.clone.modules.post.publicapi.PostNotificationMuteQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisPostNotificationMuteStore implements PostNotificationMuteStore, PostNotificationMuteQuery {
    private static final Duration MUTE_TTL = Duration.ofDays(60);

    private final ReactiveRedisTemplate<String, String> redis;

    @Override
    public Mono<Boolean> mute(String postId, String userId) {
        return redis.opsForValue().set(PostNotificationCacheKeys.mutedPostNotification(postId, userId), "true", MUTE_TTL);
    }

    @Override
    public Mono<Boolean> isMuted(String postId, String userId) {
        return redis.hasKey(PostNotificationCacheKeys.mutedPostNotification(postId, userId));
    }
}
