package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.user.service.UserVectorSnapshotService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Queue and source ownership change atomically; vector fences use the common user lease. */
@Service
public class FeedQueueCommitService {
    private static final long TTL_MILLIS = Duration.ofDays(5).toMillis();
    private final ReactiveStringRedisTemplate redis;
    private final UserVectorCoordinator coordinator;
    private final UserVectorSnapshotService snapshots;
    private final VectorRedisState vectors;
    @Value("${vector.feed.recommendation.enabled:true}")
    private boolean recommendationEnabled;

    public FeedQueueCommitService(ReactiveStringRedisTemplate redis, UserVectorCoordinator coordinator,
                                  UserVectorSnapshotService snapshots, VectorRedisState vectors) {
        this.redis = redis; this.coordinator = coordinator; this.snapshots = snapshots; this.vectors = vectors;
        if (redis.getConnectionFactory() instanceof LettuceConnectionFactory factory
                && factory.getClusterConfiguration() != null)
            throw new IllegalStateException("Feed scripts require standalone Redis; migrate key slots first");
    }

    public record QueueRead(List<String> postIds, boolean needsRefill, boolean legacy) {
        public QueueRead { postIds = List.copyOf(postIds); }
    }

    /** Task9 continuity/quarantine extends this owned-lease boundary. No ranking or hydration here. */
    public Mono<Long> requirePreServeConsistency(VectorLease lease) {
        return snapshots.load(lease).map(snapshot -> snapshot.version())
                .switchIfEmpty(Mono.defer(() -> vectors.version(lease.userId())));
    }

    public Mono<QueueRead> readForServe(String userId, int limit, Set<String> excluded) {
        return coordinator.withSnapshotLock(userId, lease -> requirePreServeConsistency(lease)
                .flatMap(version -> invalidate(lease, version))
                .flatMap(state -> redis.opsForZSet().reverseRange(FeedCacheKeys.userFeed(userId),
                                Range.closed(0L, 79L))
                        .filter(id -> id != null && !id.isBlank() && !excluded.contains(id))
                        .distinct().take(Math.max(limit, 0)).collectList()
                        .map(ids -> new QueueRead(ids, state == 1, state == 2))));
    }

    public Mono<Boolean> commit(String userId, long vectorVersion, List<FeedCandidate> candidates) {
        List<FeedCandidate> batch = List.copyOf(candidates);
        return coordinator.withSnapshotLock(userId, lease -> requirePreServeConsistency(lease)
                .flatMap(current -> {
                    if (current != vectorVersion) return Mono.just(false);
                    List<String> args = new ArrayList<>(List.of(lease.token(), Long.toString(vectorVersion),
                            Long.toString(TTL_MILLIS), readMode()));
                    for (FeedCandidate candidate : batch) {
                        if (candidate.postId() == null || candidate.postId().isBlank()) continue;
                        if (!Double.isFinite(candidate.deliveryScore()))
                            return Mono.error(new IllegalArgumentException("Non-finite candidate score"));
                        args.add(candidate.postId()); args.add(Double.toString(candidate.deliveryScore()));
                    }
                    return eval("""
                            if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end
                            if (redis.call('GET',KEYS[2]) or '0') ~= ARGV[2] then return 0 end
                            if redis.call('HGET',KEYS[7],'schema') ~= '2' and redis.call('ZCARD',KEYS[4]) > 0 then return 0 end
                            if redis.call('EXISTS',KEYS[3]) == 1 or redis.call('HGET',KEYS[7],'version') ~= ARGV[2]
                              or redis.call('HGET',KEYS[7],'mode') ~= ARGV[4] then
                              local old = redis.call('SMEMBERS',KEYS[5])
                              for _,id in ipairs(old) do
                                if redis.call('SISMEMBER',KEYS[6],id) == 0 then redis.call('ZREM',KEYS[4],id) end
                              end
                              redis.call('DEL',KEYS[5])
                            end
                            for i=5,#ARGV,2 do
                              local id = ARGV[i]
                              if redis.call('SISMEMBER',KEYS[6],id) == 0 then
                                redis.call('ZADD',KEYS[4],'NX',ARGV[i+1],id)
                                redis.call('SADD',KEYS[5],id)
                              end
                            end
                            redis.call('HSET',KEYS[7],'schema','2','version',ARGV[2],'mode',ARGV[4])
                            redis.call('DEL',KEYS[3])
                            for i=4,7 do redis.call('PEXPIRE',KEYS[i],ARGV[3]) end
                            return 1
                            """, keys(userId), args).flatMap(this::ownedResult).map(result -> result == 1);
                }));
    }

    private Mono<Long> invalidate(VectorLease lease, long version) {
        return eval("""
                if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end
                if (redis.call('GET',KEYS[2]) or '0') ~= ARGV[2] then return -1 end
                if redis.call('HGET',KEYS[7],'schema') ~= '2' then
                  if redis.call('ZCARD',KEYS[4]) > 0 then return 2 end
                  return 1
                end
                if redis.call('EXISTS',KEYS[3]) == 0 and redis.call('HGET',KEYS[7],'version') == ARGV[2]
                  and redis.call('HGET',KEYS[7],'mode') == ARGV[3] then return 0 end
                local old = redis.call('SMEMBERS',KEYS[5])
                for _,id in ipairs(old) do
                  if redis.call('SISMEMBER',KEYS[6],id) == 0 then redis.call('ZREM',KEYS[4],id) end
                end
                redis.call('DEL',KEYS[5])
                redis.call('HSET',KEYS[7],'version','-1')
                redis.call('SET',KEYS[3],ARGV[2])
                return 1
                """, keys(lease.userId()), List.of(lease.token(), Long.toString(version), readMode()))
                .flatMap(this::ownedResult);
    }

    public Mono<Void> appendFanout(String userId, String postId, Instant time) {
        double score = (time == null ? Instant.now() : time).toEpochMilli();
        return eval("""
                if redis.call('ZCARD',KEYS[1]) == 0 and redis.call('HGET',KEYS[4],'schema') ~= '2' then
                  redis.call('HSET',KEYS[4],'schema','2','version','-1')
                end
                if redis.call('SISMEMBER',KEYS[3],ARGV[1]) == 0 or not redis.call('ZSCORE',KEYS[1],ARGV[1]) then
                  redis.call('ZADD',KEYS[1],ARGV[2],ARGV[1])
                end
                redis.call('SREM',KEYS[2],ARGV[1])
                redis.call('SADD',KEYS[3],ARGV[1])
                for i=1,4 do redis.call('PEXPIRE',KEYS[i],ARGV[3]) end
                return 1
                """, List.of(FeedCacheKeys.userFeed(userId), FeedCacheKeys.recommendations(userId),
                        FeedCacheKeys.fanout(userId), FeedCacheKeys.metadata(userId)),
                List.of(postId, Double.toString(score), Long.toString(TTL_MILLIS))).then();
    }

    public Mono<Void> removeReturned(String userId, List<String> postIds) {
        if (postIds.isEmpty()) return Mono.empty();
        return eval("""
                for _,id in ipairs(ARGV) do
                  redis.call('ZREM',KEYS[1],id)
                  redis.call('SREM',KEYS[2],id)
                  redis.call('SREM',KEYS[3],id)
                end
                return 1
                """, List.of(FeedCacheKeys.userFeed(userId), FeedCacheKeys.recommendations(userId),
                        FeedCacheKeys.fanout(userId)), postIds).then();
    }

    private List<String> keys(String userId) {
        return List.of(VectorCacheKeys.lock(userId), VectorCacheKeys.version(userId), VectorCacheKeys.dirty(userId),
                FeedCacheKeys.userFeed(userId), FeedCacheKeys.recommendations(userId),
                FeedCacheKeys.fanout(userId), FeedCacheKeys.metadata(userId));
    }
    private String readMode() { return recommendationEnabled ? "recommendation" : "content"; }
    private Mono<Long> ownedResult(long result) {
        return result < 0 ? Mono.error(new UserVectorCoordinator.LeaseLostException()) : Mono.just(result);
    }
    private Mono<Long> eval(String script, List<String> keys, List<String> args) {
        return redis.execute(RedisScript.of(script, Long.class), keys, args).single();
    }
}
