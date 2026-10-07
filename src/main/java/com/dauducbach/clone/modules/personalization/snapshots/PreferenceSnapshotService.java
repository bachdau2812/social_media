package com.dauducbach.clone.modules.personalization.snapshots;

import com.dauducbach.clone.commons.vector.*;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.*;
import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import com.dauducbach.clone.modules.post.publicapi.PostAuthorPreferenceContext;
import com.dauducbach.clone.modules.post.publicapi.PostAuthorPreferenceQuery;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceQuery;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceQueueConsistency;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceQueueLease;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceSnapshot;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.List;

@Service
@RequiredArgsConstructor
public class PreferenceSnapshotService implements PreferenceQuery, PreferenceQueueConsistency, PostAuthorPreferenceQuery {
    private final UserVectorCoordinator coordinator;
    private final PreferenceRecoveryCoordinator recovery;
    private final UserVectorQueryService query;
    private final UserExistenceQuery users;
    private final VectorRedisState redis;

    @Override
    public Mono<PreferenceSnapshot> load(String userId) {
        return withSnapshotLease(userId, this::load);
    }

    @Override
    public Mono<PostAuthorPreferenceContext> loadPostAuthorContext(String userId) {
        return load(userId).map(snapshot -> new PostAuthorPreferenceContext(
                snapshot.version(), snapshot.profile(), snapshot.longTerm(), snapshot.model()));
    }

    @Override
    public <T> Mono<T> withSnapshotLease(String userId, java.util.function.Function<PreferenceQueueLease, Mono<T>> work) {
        return coordinator.withSnapshotLock(userId, lease -> work.apply(toQueueLease(lease)));
    }

    @Override
    public Mono<PreferenceSnapshot> load(PreferenceQueueLease lease) {
        return load(toVectorLease(lease));
    }

    @Override
    public Mono<Long> version(PreferenceQueueLease lease) {
        var vectorLease = toVectorLease(lease);
        return coordinator.requireOwner(vectorLease).then(redis.version(lease.userId()));
    }

    /** Caller must already own the common lease; never acquire the same lock recursively. */
    public Mono<PreferenceSnapshot> load(VectorLease lease) {
        String userId = lease.userId();
        return coordinator.requireOwner(lease)
                .then(recovery.reconcilePending(lease))
                .then(users.exists(userId)).flatMap(exists -> exists
                        ? Mono.zip(query.getSnapshot(userId).defaultIfEmpty(new UserDetailVector()), redis.version(userId), shortTerm(userId))
                            .map(tuple -> {
                                UserDetailVector doc = tuple.getT1();
                                return new PreferenceSnapshot(tuple.getT2(),
                                        compatible(doc.getUserVector(), doc.getUserVectorModel(), doc.getUserVectorDimension(), doc.getUserVectorSchemaVersion()),
                                        compatible(doc.getUserLongTermVector(), doc.getUserLongTermVectorModel(), doc.getUserLongTermVectorDimension(), doc.getUserLongTermVectorSchemaVersion()),
                                        tuple.getT3(), Boolean.TRUE.equals(doc.getHasLearnedHistory()), VectorMath.MODEL);
                            })
                        : Mono.empty());
    }
    private Mono<List<Double>> shortTerm(String userId) {
        // Legacy ST bytes have no model provenance. Skip until a known-model event replaces them atomically.
        return redis.shortTermModel(userId).filter(VectorMath.MODEL::equals)
                .flatMap(model -> redis.shortTerm(userId)).map(json -> {
                    try {
                        List<Double> vector = GsonUtils.getGson().fromJson(json, new TypeToken<List<Double>>() {}.getType());
                        return VectorMath.normalize(vector);
                    } catch (IllegalArgumentException | com.google.gson.JsonParseException error) { return List.<Double>of(); }
                }).defaultIfEmpty(List.of());
    }
    private List<Double> compatible(List<Double> vector, String model, Integer dimension, Integer schema) {
        try {
            VectorMath.requireCompatible(model, dimension, schema);
            return VectorMath.normalize(vector);
        } catch (IllegalArgumentException error) { return List.of(); }
    }

    private PreferenceQueueLease toQueueLease(VectorLease lease) {
        String userId = lease.userId();
        return new PreferenceQueueLease(userId, lease.token(), VectorCacheKeys.lock(userId),
                VectorCacheKeys.version(userId), VectorCacheKeys.dirty(userId));
    }

    private VectorLease toVectorLease(PreferenceQueueLease lease) {
        return new VectorLease(lease.userId(), lease.ownershipToken());
    }
}
