package com.dauducbach.clone.modules.personalization.infrastructure.redis;

import com.dauducbach.clone.commons.vector.VectorRepairRequiredException;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.List;

/** All mutations compare the lease token inside Redis, including the commit marker. */
@Component
public class VectorRedisState {
    private final ReactiveStringRedisTemplate redis;
    public VectorRedisState(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
        if (redis.getConnectionFactory() instanceof LettuceConnectionFactory factory
                && factory.getClusterConfiguration() != null) {
            throw new IllegalStateException("Vector scripts require standalone Redis; keys need a cluster slot migration first");
        }
    }
    public Mono<Boolean> acquire(VectorLease lease, Duration ttl) {
        return redis.opsForValue().setIfAbsent(VectorCacheKeys.lock(lease.userId()), lease.token(), ttl);
    }
    public Mono<Boolean> isOwner(VectorLease lease) {
        return redis.opsForValue().get(VectorCacheKeys.lock(lease.userId())).map(lease.token()::equals).defaultIfEmpty(false);
    }
    public Mono<Boolean> renew(VectorLease lease, Duration ttl) {
        return eval("if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end return redis.call('PEXPIRE',KEYS[1],ARGV[2])",
                List.of(VectorCacheKeys.lock(lease.userId())), lease.token(), Long.toString(ttl.toMillis())).map(value -> value == 1);
    }
    public Mono<Void> release(VectorLease lease) {
        return eval("if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end return redis.call('DEL',KEYS[1])",
                List.of(VectorCacheKeys.lock(lease.userId())), lease.token()).then();
    }
    public Mono<Long> commit(VectorLease lease, String operationId) {
        String user = lease.userId();
        return eval("""
                if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end
                if redis.call('GET',KEYS[4]) == ARGV[2] then return tonumber(redis.call('GET',KEYS[2]) or '0') end
                local version = redis.call('INCR',KEYS[2])
                redis.call('SET',KEYS[3],tostring(version))
                redis.call('SET',KEYS[4],ARGV[2])
                return version
                """, List.of(VectorCacheKeys.lock(user), VectorCacheKeys.version(user), VectorCacheKeys.dirty(user),
                VectorCacheKeys.operation(user)), lease.token(), operationId)
                .flatMap(version -> version < 0 ? Mono.error(new UserVectorCoordinator.LeaseLostException()) : Mono.just(version));
    }
    /** Remove vector metadata only; preserve the active lease and existing feed fanout data. */
    public Mono<Void> clearDeleted(VectorLease lease) {
        String user = lease.userId();
        return eval("""
                if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end
                return redis.call('DEL',KEYS[2],KEYS[3],KEYS[4],KEYS[5],KEYS[6])
                """, List.of(VectorCacheKeys.lock(user), VectorCacheKeys.version(user), VectorCacheKeys.dirty(user),
                VectorCacheKeys.operation(user), VectorCacheKeys.shortTerm(user), VectorCacheKeys.shortTermModel(user)), lease.token())
                .flatMap(result -> result < 0 ? Mono.error(new UserVectorCoordinator.LeaseLostException()) : Mono.<Void>empty());
    }
    public Mono<Long> version(String userId) {
        return redis.opsForValue().get(VectorCacheKeys.version(userId)).map(value -> {
            try {
                long version = Long.parseLong(value);
                if (version < 0) throw new NumberFormatException("negative version");
                return version;
            } catch (NumberFormatException error) {
                throw new VectorRepairRequiredException("Redis vector version is malformed; quarantine user and inspect durable state", error);
            }
        }).defaultIfEmpty(0L);
    }
    public Mono<String> operation(String userId) { return redis.opsForValue().get(VectorCacheKeys.operation(userId)); }
    public Mono<String> shortTerm(String userId) { return redis.opsForValue().get(VectorCacheKeys.shortTerm(userId)); }
    public Mono<String> shortTermModel(String userId) { return redis.opsForValue().get(VectorCacheKeys.shortTermModel(userId)); }
    private Mono<Long> eval(String script, List<String> keys, String... arguments) {
        return redis.execute(RedisScript.of(script, Long.class), keys, List.of(arguments)).single();
    }
}
