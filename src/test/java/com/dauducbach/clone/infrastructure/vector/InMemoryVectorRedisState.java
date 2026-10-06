package com.dauducbach.clone.infrastructure.vector;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import static org.mockito.Mockito.mock;

/** Stateful infrastructure double; production orchestration remains real. */
public class InMemoryVectorRedisState extends VectorRedisState {
    public String owner;
    public int renewals;
    public long version;
    public String committed;
    public boolean failCommit;
    public String shortJson;
    public String shortModel;
    public final Map<String, String> locks = new HashMap<>();
    public InMemoryVectorRedisState() { super(mock(ReactiveStringRedisTemplate.class)); }
    @Override public Mono<Boolean> acquire(VectorLease lease, Duration ttl) {
        return Mono.fromSupplier(() -> { if (owner != null) return false; owner = lease.token(); return true; });
    }
    @Override public Mono<Boolean> isOwner(VectorLease lease) { return Mono.fromSupplier(() -> lease.token().equals(owner)); }
    @Override public Mono<Boolean> renew(VectorLease lease, Duration ttl) {
        return isOwner(lease).doOnNext(ok -> { if (ok) renewals++; });
    }
    @Override public Mono<Void> release(VectorLease lease) {
        return Mono.fromRunnable(() -> { if (lease.token().equals(owner)) owner = null; });
    }
    @Override public Mono<Long> commit(VectorLease lease, String operationId) {
        return isOwner(lease).flatMap(ok -> {
            if (!ok) return Mono.error(new UserVectorCoordinator.LeaseLostException());
            if (failCommit) return Mono.error(new IllegalStateException("Redis unavailable"));
            if (!operationId.equals(committed)) { committed = operationId; version++; }
            return Mono.just(version);
        });
    }
    @Override public Mono<Long> version(String user) { return Mono.fromSupplier(() -> version); }
    @Override public Mono<String> shortTerm(String user) { return Mono.defer(() -> Mono.justOrEmpty(shortJson)); }
    @Override public Mono<String> shortTermModel(String user) { return Mono.defer(() -> Mono.justOrEmpty(shortModel)); }
    @Override public Mono<Void> clearDeleted(VectorLease lease) {
        return isOwner(lease).flatMap(ok -> {
            if (!ok) return Mono.error(new UserVectorCoordinator.LeaseLostException());
            version = 0; committed = null; shortJson = null; shortModel = null;
            return Mono.empty();
        });
    }

}
