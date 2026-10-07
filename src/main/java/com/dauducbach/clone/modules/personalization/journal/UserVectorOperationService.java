package com.dauducbach.clone.modules.personalization.journal;

import com.dauducbach.clone.commons.vector.*;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.*;
import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.model.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.personalization.infrastructure.persistence.UserVectorUpdateOperationRepository;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.recovery.UserVectorContinuityService;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Caller holds the common lease. A durable desired vector is replayed against its original OCC tokens. */
@Service
@RequiredArgsConstructor
public class UserVectorOperationService {
    private final UserVectorUpdateOperationRepository repository;
    private final UserExistenceQuery users;
    private final UserVectorQueryService query;
    private final UserVectorStore store;
    private final UserVectorCoordinator coordinator;
    private final VectorRedisState redis;
    private final UserVectorContinuityService continuity;

    public Flux<UserVectorUpdateOperation> findPending(String userId) { return repository.findPending(userId); }
    /** Read checkpoint by stable logical key; callers reconcile under the lease before making write decisions. */
    public Mono<UserVectorUpdateOperation> findByOperationKey(String key) { return repository.findByOperationKey(key); }
    public Mono<Void> reconcilePending(VectorLease lease) {
        return coordinator.requireOwner(lease).thenMany(findPending(lease.userId()))
                .concatMap(operation -> recover(lease, operation)).then()
                .then(Mono.defer(() -> continuity.requireContinuity(lease)));
    }
    /** Deletion cleanup must cancel pending rows without replaying their vectors. Caller holds the common lease. */
    public Mono<Void> cancelForDeleted(VectorLease lease) {
        return coordinator.requireOwner(lease).then(users.exists(lease.userId())).flatMap(exists -> {
            if (exists) return Mono.error(new IllegalStateException("Cannot cancel vector work for an existing user"));
            return findPending(lease.userId()).concatMap(operation -> transition(lease, operation, "DELETED")).then();
        });
    }
    public Mono<UserVectorUpdateOperation> applyProfile(VectorLease lease, String key, List<Double> desired) {
        return query.getSnapshot(lease.userId()).defaultIfEmpty(new UserDetailVector())
                .flatMap(baseline -> prepare(lease, key, "PROFILE", baseline, desired, "profile-v1", null, null))
                .flatMap(operation -> recover(lease, operation));
    }
    public Mono<UserVectorUpdateOperation> applyLongTerm(VectorLease lease, String key, UserDetailVector captured,
            List<Double> desired, String formula, Instant from, Instant to) {
        return prepare(lease, key, "LONG_TERM", captured, desired, formula, from, to)
                .flatMap(operation -> recover(lease, operation));
    }
    public Mono<UserVectorUpdateOperation> checkpoint(VectorLease lease, String key, String formula, Instant from, Instant to) {
        return prepare(lease, key, "CHECKPOINT", new UserDetailVector(), List.of(), formula, from, to)
                .flatMap(operation -> recover(lease, operation));
    }
    public Mono<UserVectorUpdateOperation> prepare(VectorLease lease, String key, String kind, UserDetailVector baseline,
            List<Double> desired, String formula, Instant from, Instant to) {
        return Mono.defer(() -> {
            if (key == null || key.isBlank() || !List.of("PROFILE", "LONG_TERM", "CHECKPOINT").contains(kind)
                    || formula == null || formula.isBlank()) return Mono.error(new IllegalArgumentException("Invalid vector operation"));
            List<Double> normalized = kind.equals("CHECKPOINT") ? List.of() : VectorMath.normalize(desired);
            return reconcilePending(lease).then(requireUser(lease)).then(repository.findByOperationKey(key))
                    .flatMap(existing -> {
                        // A conflicted LT attempt never applied its stale desired vector. A fresh no-signal
                        // result may close that same range as a checkpoint, without an ES/history write.
                        boolean noSignalCheckpoint = "CONFLICTED".equals(existing.getStatus())
                                && "LONG_TERM".equals(existing.getOperationKind()) && "CHECKPOINT".equals(kind);
                        if (!existing.getUserId().equals(lease.userId())
                                || (!existing.getOperationKind().equals(kind) && !noSignalCheckpoint)
                                || !Objects.equals(existing.getFormulaVersion(), formula)
                                || !Objects.equals(existing.getRangeFrom(), from) || !Objects.equals(existing.getRangeTo(), to))
                            return Mono.error(new IllegalArgumentException("Operation key belongs to a different user/kind/range/formula"));
                        if (!existing.getStatus().equals("CONFLICTED")) return Mono.just(existing);
                        // The original OCC tokens can no longer write. Reuse the logical key with a fresh attempt.
                        return validateBaseline(lease, kind, baseline).then(Mono.defer(() -> {
                            existing.setOperationKind(kind);
                            existing.setOperationId(UUID.randomUUID().toString()); existing.setStatus("PREPARED");
                            existing.setBaselineVector(json(kind.equals("PROFILE") ? baseline.getUserVector() : baseline.getUserLongTermVector()));
                            existing.setDesiredVector(json(normalized));
                            existing.setBaselineSeqNo(baseline.getSeqNoPrimaryTerm() == null ? null : baseline.getSeqNoPrimaryTerm().sequenceNumber());
                            existing.setBaselinePrimaryTerm(baseline.getSeqNoPrimaryTerm() == null ? null : baseline.getSeqNoPrimaryTerm().primaryTerm());
                            existing.setUpdatedAt(Instant.now());
                            return coordinator.requireOwner(lease).then(repository.save(existing));
                        }));
                    })
                    .switchIfEmpty(Mono.defer(() -> validateBaseline(lease, kind, baseline).then(Mono.defer(() -> {
                        UserVectorUpdateOperation operation = new UserVectorUpdateOperation();
                        operation.setOperationKey(key); operation.setUserId(lease.userId()); operation.setOperationKind(kind);
                        operation.setOperationId(UUID.randomUUID().toString()); operation.setFormulaVersion(formula);
                        operation.setBaselineVector(json(kind.equals("PROFILE") ? baseline.getUserVector() : baseline.getUserLongTermVector()));
                        operation.setDesiredVector(json(normalized));
                        if (baseline.getSeqNoPrimaryTerm() != null) {
                            operation.setBaselineSeqNo(baseline.getSeqNoPrimaryTerm().sequenceNumber());
                            operation.setBaselinePrimaryTerm(baseline.getSeqNoPrimaryTerm().primaryTerm());
                        }
                        operation.setRangeFrom(from); operation.setRangeTo(to); operation.setStatus("PREPARED");
                        operation.setCreatedAt(Instant.now()); operation.setUpdatedAt(operation.getCreatedAt());
                        return repository.save(operation).onErrorResume(DuplicateKeyException.class, error ->
                                repository.findByOperationKey(key).flatMap(existing -> existing.getUserId().equals(lease.userId())
                                        && existing.getOperationKind().equals(kind) ? Mono.just(existing) : Mono.error(error)));
                    }))));
        });
    }
    private Mono<Void> validateBaseline(VectorLease lease, String kind, UserDetailVector baseline) {
        if (kind.equals("CHECKPOINT")) return Mono.empty();
        return query.getSnapshot(lease.userId()).defaultIfEmpty(new UserDetailVector()).flatMap(current ->
                Objects.equals(current.getSeqNoPrimaryTerm(), baseline.getSeqNoPrimaryTerm())
                        && (!kind.equals("LONG_TERM") || baseline.getSeqNoPrimaryTerm() != null)
                        ? Mono.empty() : Mono.error(conflict()));
    }
    public Mono<UserVectorUpdateOperation> markEsApplied(VectorLease lease, UserVectorUpdateOperation operation) {
        if (operation.getStatus().equals("COMPLETED")) return coordinator.requireOwner(lease).thenReturn(operation);
        return transition(lease, operation, "ES_APPLIED");
    }
    public Mono<UserVectorUpdateOperation> complete(VectorLease lease, UserVectorUpdateOperation operation) {
        return Mono.defer(() -> {
            if (!lease.userId().equals(operation.getUserId())) return Mono.error(new IllegalArgumentException("Lease belongs to another user"));
            // Redis tracks only the latest attempt. SQL optimistic locking after publication cannot undo
            // republishing an older attempt, so always resolve a caller's retained copy before side effects.
            return coordinator.requireOwner(lease).then(repository.findByOperationKey(operation.getOperationKey()))
                    .switchIfEmpty(Mono.error(new OptimisticLockingFailureException("Vector operation no longer exists")))
                    .flatMap(durable -> {
                        if (!Objects.equals(durable.getId(), operation.getId())
                                || !Objects.equals(durable.getUserId(), lease.userId())
                                || !Objects.equals(durable.getOperationKey(), operation.getOperationKey())
                                || !Objects.equals(durable.getOperationKind(), operation.getOperationKind())
                                || !Objects.equals(durable.getOperationId(), operation.getOperationId()))
                            return Mono.error(new OptimisticLockingFailureException("Vector operation attempt was superseded"));
                        // Completion is terminal for this attempt, even if the caller retained an older row version.
                        if (durable.getStatus().equals("COMPLETED")) return coordinator.requireOwner(lease).thenReturn(durable);
                        if (!Objects.equals(durable.getRowVersion(), operation.getRowVersion()))
                            return Mono.error(new OptimisticLockingFailureException("Stale vector operation row version"));
                        if (!durable.getStatus().equals("ES_APPLIED")
                                && !(durable.getOperationKind().equals("CHECKPOINT") && durable.getStatus().equals("PREPARED")))
                            return Mono.error(new IllegalStateException("Cannot commit a vector before ES application is confirmed"));
                        return requireUser(lease)
                                .then(durable.getOperationKind().equals("CHECKPOINT") ? Mono.empty() : redis.commit(lease, durable.getOperationId()).then())
                                .then(transition(lease, durable, "COMPLETED"));
                    });
        });
    }
    private Mono<UserVectorUpdateOperation> recover(VectorLease lease, UserVectorUpdateOperation operation) {
        return coordinator.requireOwner(lease).then(users.exists(lease.userId())).flatMap(exists -> {
            if (!exists) return transition(lease, operation, "DELETED").then(Mono.error(new IllegalStateException("User deleted")));
            if (operation.getStatus().equals("COMPLETED")) return Mono.just(operation);
            if (operation.getStatus().equals("CONFLICTED")) return Mono.error(conflict());
            if (operation.getStatus().equals("DELETED")) return Mono.error(new IllegalStateException("User deleted"));
            if (operation.getStatus().equals("ES_APPLIED") || operation.getOperationKind().equals("CHECKPOINT"))
                return complete(lease, operation);
            return query.getSnapshot(lease.userId()).defaultIfEmpty(new UserDetailVector()).flatMap(current -> {
                if (applied(current, operation)) return markEsApplied(lease, operation).flatMap(applied -> complete(lease, applied));
                if (!sameBaseline(current, operation)) return transition(lease, operation, "CONFLICTED").then(Mono.error(conflict()));
                return coordinator.requireOwner(lease).then(write(operation))
                        // RPC failure is ambiguous: read op ID, never assume ES did not commit.
                        .onErrorResume(error -> query.getSnapshot(lease.userId()).filter(doc -> applied(doc, operation))
                                .switchIfEmpty(Mono.error(error)).then())
                        .then(markEsApplied(lease, operation)).flatMap(applied -> complete(lease, applied));
            });
        });
    }
    private Mono<Void> write(UserVectorUpdateOperation operation) {
        List<Double> desired = GsonUtils.getGson().fromJson(operation.getDesiredVector(), new TypeToken<List<Double>>() {}.getType());
        if (operation.getOperationKind().equals("PROFILE")) return store.upsertProfileAndSeedLongTerm(operation.getUserId(), desired,
                operation.getOperationId(), operation.getBaselineSeqNo(), operation.getBaselinePrimaryTerm());
        return store.updateLongTerm(operation.getUserId(), desired, operation.getOperationId(),
                operation.getBaselineSeqNo(), operation.getBaselinePrimaryTerm());
    }
    private boolean applied(UserDetailVector current, UserVectorUpdateOperation operation) {
        return operation.getOperationId().equals(operation.getOperationKind().equals("PROFILE")
                ? current.getUserProfileOperationId() : current.getUserLongTermOperationId());
    }
    private boolean sameBaseline(UserDetailVector current, UserVectorUpdateOperation operation) {
        return current.getSeqNoPrimaryTerm() == null ? operation.getBaselineSeqNo() == null
                : Objects.equals(current.getSeqNoPrimaryTerm().sequenceNumber(), operation.getBaselineSeqNo())
                && Objects.equals(current.getSeqNoPrimaryTerm().primaryTerm(), operation.getBaselinePrimaryTerm());
    }
    private Mono<Void> requireUser(VectorLease lease) {
        return coordinator.requireOwner(lease).then(users.exists(lease.userId()))
                .flatMap(exists -> exists ? Mono.empty() : Mono.error(new IllegalStateException("User deleted")));
    }
    private Mono<UserVectorUpdateOperation> transition(VectorLease lease, UserVectorUpdateOperation operation, String status) {
        if (!lease.userId().equals(operation.getUserId())) return Mono.error(new IllegalArgumentException("Lease belongs to another user"));
        return coordinator.requireOwner(lease).then(Mono.defer(() -> {
            operation.setStatus(status); operation.setUpdatedAt(Instant.now()); return repository.save(operation);
        }));
    }
    private String json(List<Double> vector) { return GsonUtils.getGson().toJson(vector == null ? List.of() : vector); }
    private UserVectorStore.RetryableVectorConflictException conflict() {
        return new UserVectorStore.RetryableVectorConflictException(new IllegalStateException("Captured vector baseline changed"));
    }
}
