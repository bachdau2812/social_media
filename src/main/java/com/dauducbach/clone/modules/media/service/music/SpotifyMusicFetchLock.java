package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;

@Component
public class SpotifyMusicFetchLock {
    private static final String KEY_PREFIX = "music:fetch:lock:";
    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>(
            """
            if redis.call('get', KEYS[1]) == ARGV[1] then
                return redis.call('del', KEYS[1])
            end
            return 0
            """,
            Long.class);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final SpotifyMusicFetchProperties properties;

    public SpotifyMusicFetchLock(
            ReactiveStringRedisTemplate redisTemplate,
            SpotifyMusicFetchProperties properties) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
    }

    public Mono<Boolean> tryAcquire(String trackId, String token) {
        return redisTemplate.opsForValue()
                .setIfAbsent(key(trackId), token, properties.getLockTtl())
                .defaultIfEmpty(false);
    }

    public Mono<Boolean> release(String trackId, String token) {
        return redisTemplate.execute(
                        RELEASE_SCRIPT,
                        List.of(key(trackId)),
                        List.of(token))
                .next()
                .map(deleted -> deleted != null && deleted > 0)
                .defaultIfEmpty(false);
    }

    private String key(String trackId) {
        return KEY_PREFIX + trackId;
    }
}
