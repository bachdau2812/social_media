package com.dauducbach.clone.modules.personalization.infrastructure.redis;

import com.dauducbach.clone.modules.personalization.publicapi.PreferenceLeaseLostException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;

@Component
public class UserVectorCoordinator {
    private static final Logger log = LoggerFactory.getLogger(UserVectorCoordinator.class);
    private static final Duration LOCK_WAIT = Duration.ofSeconds(10);
    private final VectorRedisState redis;
    private final Duration ttl;
    private final Duration renewal;
    @Autowired
    public UserVectorCoordinator(VectorRedisState redis) { this(redis, Duration.ofSeconds(15), Duration.ofSeconds(5)); }
    UserVectorCoordinator(VectorRedisState redis, Duration ttl, Duration renewal) {
        this.redis = redis; this.ttl = ttl; this.renewal = renewal;
    }
    public <T> Mono<T> withUserLock(String userId, Function<VectorLease, Mono<T>> operation) {
        return withLock(userId, LOCK_WAIT, operation);
    }
    public <T> Mono<T> withSnapshotLock(String userId, Function<VectorLease, Mono<T>> operation) {
        // Snapshot reads share the lease with feed recovery and vector writes over remote infrastructure.
        return withLock(userId, LOCK_WAIT, operation);
    }
    private <T> Mono<T> withLock(String userId, Duration wait, Function<VectorLease, Mono<T>> operation) {
        return Mono.defer(() -> {
            if (userId == null || userId.isBlank()) return Mono.error(new IllegalArgumentException("Missing user ID"));
            VectorLease lease = new VectorLease(userId, UUID.randomUUID().toString());
            Mono<VectorLease> acquire = Mono.defer(() -> redis.acquire(lease, ttl))
                    .filter(Boolean.TRUE::equals).repeatWhenEmpty(repeats -> repeats.delayElements(Duration.ofMillis(50)))
                    .thenReturn(lease).timeout(wait)
                    .doOnError(java.util.concurrent.TimeoutException.class, error -> log.warn(
                            "|UserVectorCoordinator|lockWaitTimeout|userId={}|waitMs={}", userId, wait.toMillis()));
            return Mono.usingWhen(acquire, owned -> {
                Mono<T> heartbeat = Flux.interval(renewal).concatMap(tick -> redis.renew(owned, ttl).timeout(renewal)
                        .flatMap(ok -> ok ? Mono.empty() : Mono.error(new LeaseLostException())))
                        .then(Mono.<T>never());
                Mono<T> work = requireOwner(owned).then(Mono.defer(() -> operation.apply(owned)))
                        .materialize().flatMap(signal -> signal.isOnError() ? Mono.just(signal)
                                : requireOwner(owned).thenReturn(signal)).dematerialize();
                return Mono.firstWithSignal(heartbeat, work);
            }, redis::release, (owned, error) -> redis.release(owned), redis::release);
        });
    }
    public Mono<Boolean> isOwner(VectorLease lease) { return redis.isOwner(lease); }
    public Mono<Void> requireOwner(VectorLease lease) {
        return isOwner(lease).flatMap(ok -> ok ? Mono.empty() : Mono.error(new LeaseLostException()));
    }
    public static class LeaseLostException extends PreferenceLeaseLostException {
        public LeaseLostException() { super(); }
    }
}
