package com.dauducbach.clone.modules.personalization.profile;

import com.dauducbach.clone.commons.vector.*;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.*;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.profile.UserProfileVectorRefreshUseCase;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Callable service only. Applying is an explicit operator action, never triggered by a read. */
@Service
@RequiredArgsConstructor
public class UserVectorRepairService {
    private final UserExistenceQuery users;
    private final UserVectorQueryService query;
    private final UserVectorCoordinator coordinator;
    private final PreferenceRecoveryCoordinator recovery;
    private final UserVectorOperationService operations;
    private final UserProfileVectorRefreshUseCase profiles;

    public record Inventory(String userId, List<String> findings, boolean seedAllowed, boolean reembedProfileRequired) {
        public Inventory { findings = List.copyOf(findings); }
    }
    public record Result(String userId, String status, String detail) {}

    /** Read-only inventory. Connection failures propagate; they are never labeled missing documents. */
    public Flux<Inventory> dryRun(List<String> userIds) {
        return Flux.fromIterable(VectorRepairBatch.ids(userIds)).concatMap(this::inspect, 1);
    }

    /** Sequential bounded batches. Re-embedding requires the explicit boolean; provider work runs outside the lease. */
    public Flux<Result> apply(List<String> userIds, Duration interval, boolean reembedProfile) {
        List<String> ids = VectorRepairBatch.ids(userIds);
        Duration delay = VectorRepairBatch.interval(interval);
        return Flux.fromIterable(ids).concatMap(id -> Mono.delay(delay).then(applyOne(id, reembedProfile))
                .onErrorResume(VectorRepairRequiredException.class, error -> Mono.just(new Result(id, "QUARANTINED", error.getMessage())))
                .onErrorResume(error -> Mono.just(new Result(id, "FAILED", error.getClass().getSimpleName()))), 1);
    }

    private Mono<Inventory> inspect(String userId) {
        return users.exists(userId).flatMap(exists -> query.getSnapshot(userId)
                .defaultIfEmpty(new UserDetailVector()).map(doc -> inventory(userId, exists, doc)));
    }

    private Inventory inventory(String id, boolean exists, UserDetailVector document) {
        List<String> findings = new ArrayList<>();
        if (!exists) findings.add("USER_DELETED");
        if (document.getUserId() == null) findings.add("DOCUMENT_MISSING");
        if (Boolean.TRUE.equals(document.getDeleted())) findings.add("ES_TOMBSTONE");
        boolean profileValid = inspectVector("PROFILE", document.getUserVector(), document.getUserVectorModel(),
                document.getUserVectorDimension(), document.getUserVectorSchemaVersion(), findings);
        boolean longTermMissing = document.getUserLongTermVector() == null || document.getUserLongTermVector().isEmpty();
        inspectVector("LONG_TERM", document.getUserLongTermVector(), document.getUserLongTermVectorModel(),
                document.getUserLongTermVectorDimension(), document.getUserLongTermVectorSchemaVersion(), findings);
        boolean lostLearned = longTermMissing && Boolean.TRUE.equals(document.getHasLearnedHistory());
        if (lostLearned) findings.add("LEARNED_LONG_TERM_MISSING");
        if (findings.isEmpty()) findings.add("READY");
        boolean live = exists && !Boolean.TRUE.equals(document.getDeleted());
        return new Inventory(id, findings, live && longTermMissing && !lostLearned && profileValid, live && !profileValid);
    }

    private boolean inspectVector(String name, List<Double> vector, String model, Integer dimension, Integer schema, List<String> findings) {
        if (vector == null || vector.isEmpty()) { findings.add(name + "_MISSING"); return false; }
        try { VectorMath.requireCompatible(model, dimension, schema); }
        catch (IllegalArgumentException error) {
            findings.add(name + (model == null || dimension == null || schema == null ? "_MODEL_UNKNOWN" : "_MODEL_MISMATCH"));
            return false;
        }
        try { VectorMath.normalize(vector); return true; }
        catch (IllegalArgumentException error) { findings.add(name + "_VECTOR_INVALID"); return false; }
    }

    private Mono<Result> applyOne(String userId, boolean reembedProfile) {
        return inspect(userId).flatMap(inventory -> {
            if (inventory.findings().contains("USER_DELETED")) return result(userId, "SKIPPED_USER_DELETED", "No source profile");
            if (inventory.findings().contains("ES_TOMBSTONE") || inventory.findings().contains("LEARNED_LONG_TERM_MISSING"))
                return result(userId, "SKIPPED_OPERATOR_RECOVERY_REQUIRED", "Retain learned state and deletion fences");
            if (inventory.findings().stream().anyMatch(f -> f.startsWith("LONG_TERM_MODEL_") || f.equals("LONG_TERM_VECTOR_INVALID")))
                return result(userId, "SKIPPED_LONG_TERM_REPAIR_REQUIRED", "Nonempty LT is preserved; model cannot be guessed from dimension");
            if (inventory.seedAllowed()) return seed(userId);
            if (inventory.reembedProfileRequired()) return reembedProfile
                    ? profiles.refreshUserVector(userId).then(inspect(userId)).flatMap(after -> result(userId,
                        after.findings().contains("USER_DELETED") ? "SKIPPED_USER_DELETED" : after.reembedProfileRequired() ? "SOURCE_CHANGED_RETRY_REQUIRED" : "PROFILE_REEMBEDDED",
                        "Profile refresh uses source checks/journal; nonempty LT preserved"))
                    : result(userId, "SKIPPED_PROFILE_REEMBED_REQUIRED", "Unknown profile provenance is never relabeled");
            return result(userId, "NO_CHANGE", "Existing LT retained");
        });
    }

    private Mono<Result> seed(String userId) {
        return coordinator.withUserLock(userId, lease -> recovery.reconcilePending(lease)
                .then(users.exists(userId)).flatMap(exists -> query.getSnapshot(userId).defaultIfEmpty(new UserDetailVector())
                        .flatMap(document -> {
                            Inventory fresh = inventory(userId, exists, document);
                            if (!fresh.seedAllowed()) return result(userId, "NO_CHANGE", "Source changed; inventory again");
                            return operations.applyProfile(lease, "repair-seed:" + UUID.randomUUID(), VectorMath.normalize(document.getUserVector()))
                                    .thenReturn(new Result(userId, "SEEDED_LONG_TERM", "Compatible profile only; existing LT never replaced"));
                        })));
    }

    private Mono<Result> result(String userId, String status, String detail) { return Mono.just(new Result(userId, status, detail)); }
}
