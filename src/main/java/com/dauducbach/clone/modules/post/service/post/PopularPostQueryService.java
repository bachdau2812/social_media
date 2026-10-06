package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.post.dto.response.PopularPostRef;
import com.dauducbach.clone.modules.post.dto.event.PostEventJson;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

@Service
public class PopularPostQueryService {
    private static final RedisScript<String> QUERY;
    static {
        DefaultRedisScript<String> script = new DefaultRedisScript<>();
        script.setLocation(new ClassPathResource("redis/query_post_popularity.lua")); script.setResultType(String.class);
        QUERY = script;
    }
    private final ReactiveRedisTemplate<String, String> redis;
    private final PostPopularityProperties properties;

    public PopularPostQueryService(ReactiveRedisTemplate<String, String> redis, PostPopularityProperties properties) {
        this.redis = redis; this.properties = properties;
    }

    public Mono<List<PopularPostRef>> findPopularPosts(Instant upperTimeBound, Instant now, PopularPostRef after, int scanLimit) {
        if (scanLimit < 1 || scanLimit > properties.getMaxScanPerSource()) return Mono.error(new IllegalArgumentException("Invalid popular scan limit"));
        return redis.execute(QUERY, List.of(properties.getRedisKey()), List.of(Long.toString(upperTimeBound.toEpochMilli()),
                        Long.toString(now.toEpochMilli() - properties.getPopularLifetime().toMillis()),
                        after == null ? "-1" : Long.toString(after.popularSinceMillis()), after == null ? "" : after.postId(), Integer.toString(scanLimit)))
                .single().map(json -> List.of(PostEventJson.read(json, PopularPostRef[].class)));
    }
}
