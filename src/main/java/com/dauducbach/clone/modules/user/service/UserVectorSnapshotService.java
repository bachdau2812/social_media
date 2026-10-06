package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.user.dto.UserVectorSnapshot;
import com.dauducbach.clone.modules.user.entity.UserDetailVector;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.feed.service.FeedInteractionProcessingService;
import org.springframework.beans.factory.ObjectProvider;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.List;

@Service
@RequiredArgsConstructor
public class UserVectorSnapshotService {
    private final UserVectorCoordinator coordinator;
    private final UserVectorOperationService operations;
    private final UserVectorQueryService query;
    private final UserDetailsRepository users;
    private final VectorRedisState redis;
    // Deferred resolution avoids the processing -> feed-vector -> shared-snapshot dependency cycle.
    private final ObjectProvider<FeedInteractionProcessingService> shortTermProcessing;

    public Mono<UserVectorSnapshot> load(String userId) {
        return coordinator.withSnapshotLock(userId, this::load);
    }

    /** Caller must already own the common lease; never acquire the same lock recursively. */
    public Mono<UserVectorSnapshot> load(VectorLease lease) {
        String userId = lease.userId();
        return coordinator.requireOwner(lease)
                .then(operations.reconcilePending(lease))
                .then(Mono.defer(() -> shortTermProcessing.getObject().reconcilePending(lease)))
                .then(Mono.defer(() -> shortTermProcessing.getObject().requireContinuity(lease)))
                .then(users.existsById(userId)).flatMap(exists -> exists
                        ? Mono.zip(query.getSnapshot(userId).defaultIfEmpty(new UserDetailVector()), redis.version(userId), shortTerm(userId))
                            .map(tuple -> {
                                UserDetailVector doc = tuple.getT1();
                                return new UserVectorSnapshot(tuple.getT2(),
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
}
