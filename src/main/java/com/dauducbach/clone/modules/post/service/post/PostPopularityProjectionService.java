package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.event.PostPopularityUpdate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Clock;
import java.time.Instant;
import java.util.List;

@Service
public class PostPopularityProjectionService {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(PostPopularityProjectionService.class);
    private static final RedisScript<Long> UPSERT = script("redis/upsert_post_popularity.lua");
    private static final RedisScript<Long> PRUNE = RedisScript.of("return redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',ARGV[1])", Long.class);
    private final ReactiveRedisTemplate<String, String> redis;
    private final PostPopularityProperties properties;
    private final Clock clock;

    @Autowired
    public PostPopularityProjectionService(ReactiveRedisTemplate<String, String> redis, PostPopularityProperties properties) {
        this(redis, properties, Clock.systemUTC());
    }

    public PostPopularityProjectionService(ReactiveRedisTemplate<String, String> redis, PostPopularityProperties properties, Clock clock) {
        this.redis = redis; this.properties = properties; this.clock = clock;
    }

    public Mono<Void> apply(PostPopularityUpdate update) {
        return Mono.defer(() -> {
            Instant now = clock.instant();
            if (update == null || update.schemaVersion() != 1 || update.postId() == null || update.postId().isBlank()
                    || update.popularSince() == null || update.expiresAt() == null || update.popularSince().isBefore(Instant.EPOCH)
                    || update.popularSince().isAfter(now.plusSeconds(60))
                    || !update.expiresAt().equals(update.popularSince().plus(properties.getPopularLifetime()))
                    || update.qualificationScore() <= properties.getThreshold() || !"popular-v1".equals(update.algorithmVersion())
                    || !("POPULAR_V1:" + update.postId() + ":" + update.popularSince().toEpochMilli()).equals(update.eventId())) {
                return Mono.error(new IllegalArgumentException("Invalid popularity update"));
            }
            if (!update.expiresAt().isAfter(now)) return Mono.empty();
            return redis.execute(UPSERT, List.of(properties.getRedisKey()), List.of(update.postId(),
                            Long.toString(update.popularSince().toEpochMilli()), Long.toString(now.toEpochMilli()),
                            Long.toString(properties.getPopularLifetime().toMillis())))
                    .single().flatMap(result -> result < 0 ? Mono.error(new IllegalArgumentException("Invalid promotion time")) : Mono.empty());
        });
    }

    @Scheduled(fixedDelayString = "${post.popularity.prune-delay-ms:60000}")
    public void pruneExpired() {
        if (!properties.isProjectionEnabled()) return;
        redis.execute(PRUNE, List.of(properties.getRedisKey()), List.of(Long.toString(clock.millis() - properties.getPopularLifetime().toMillis())))
                .subscribe(count -> LOG.debug("Popularity pruned {} expired members", count), error -> LOG.warn("Popularity prune failed", error));
    }

    private static RedisScript<Long> script(String path) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource(path)); script.setResultType(Long.class);
        return script;
    }
}
