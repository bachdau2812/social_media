package com.dauducbach.clone.modules.personalization.interactions;

import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.modules.personalization.infrastructure.persistence.PreferenceInteractionProcessingRepository;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.ShortTermPreferenceRedisState;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorRedisState;
import com.dauducbach.clone.modules.personalization.model.PreferenceInteractionProcessing;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/** Replays the durable short-term interaction ledger and verifies its Redis cursor under the user lease. */
@Service
@RequiredArgsConstructor
public class PreferenceInteractionRecoveryService implements PreferenceInteractionRecovery {
    private static final org.slf4j.Logger log =
            org.slf4j.LoggerFactory.getLogger(PreferenceInteractionRecoveryService.class);

    private final PreferenceInteractionProcessingRepository ledger;
    private final UserVectorCoordinator coordinator;
    private final VectorRedisState redis;
    private final ShortTermPreferenceRedisState shortRedis;
    private final UserExistenceQuery users;

    /** Replay only. The recovery coordinator owns continuity checks around this work. */
    @Override
    public Mono<Void> reconcilePending(VectorLease lease) {
        return coordinator.requireOwner(lease)
                .thenMany(ledger.findPending(lease.userId()))
                .concatMap(row -> finishPrepared(lease, row), 1)
                .then();
    }

    private Mono<Void> finishPrepared(VectorLease lease, PreferenceInteractionProcessing row) {
        if ("SHORT_APPLIED".equals(row.getStatus())) {
            return coordinator.requireOwner(lease).then(saveStatus(row, "COMPLETED")).then();
        }
        if (!"PREPARED".equals(row.getStatus()) && !"SKIP_PREPARED".equals(row.getStatus())) {
            return Mono.error(new IllegalStateException("Unexpected processing state: " + row.getStatus()));
        }
        return prepareDeletedSkip(lease, row)
                .flatMap(prepared -> Mono.zip(
                                shortRedis.cursor(lease.userId()),
                                redis.version(lease.userId()))
                        .flatMap(state -> users.exists(lease.userId()).flatMap(exists -> {
                            // User deletion clears ST/version; never restore an already applied snapshot.
                            if (!exists && !state.getT1().isEmpty()
                                    && ShortTermPreferenceRedisState.Cursor.parse(state.getT1())
                                            .operationId().equals(prepared.getOperationId())) {
                                return coordinator.requireOwner(lease).thenReturn(state.getT2());
                            }
                            return shortRedis.commit(lease, prepared, state.getT1(), state.getT2());
                        })))
                .then(shortRedis.cursor(lease.userId()))
                .flatMap(value -> {
                    ShortTermPreferenceRedisState.Cursor cursor = ShortTermPreferenceRedisState.Cursor.parse(value);
                    if (!cursor.matches(row) || !cursor.operationId().equals(row.getOperationId())) {
                        return Mono.error(continuity("Committed ST cursor differs from prepared operation"));
                    }
                    if ("SKIP_PREPARED".equals(row.getStatus())) {
                        return coordinator.requireOwner(lease).then(saveStatus(row, "SKIPPED"));
                    }
                    if ("EXPIRED".equals(cursor.mode())) {
                        row.setReason("DESIRED_SNAPSHOT_EXPIRED_BEFORE_COMMIT");
                    }
                    return coordinator.requireOwner(lease)
                            .then(saveStatus(row, "SHORT_APPLIED"))
                            .flatMap(saved -> coordinator.requireOwner(lease).then(saveStatus(saved, "COMPLETED")));
                })
                .then();
    }

    private Mono<PreferenceInteractionProcessing> prepareDeletedSkip(
            VectorLease lease,
            PreferenceInteractionProcessing row) {
        if (!"PREPARED".equals(row.getStatus())) {
            return Mono.just(row);
        }
        return users.exists(lease.userId()).flatMap(exists -> {
            if (exists) {
                return Mono.just(row);
            }
            return shortRedis.cursor(lease.userId()).flatMap(value -> {
                // Already acknowledged before deletion: finish its durable state without writing it again.
                if (!value.isEmpty()
                        && ShortTermPreferenceRedisState.Cursor.parse(value)
                                .operationId().equals(row.getOperationId())) {
                    return Mono.just(row);
                }
                row.setReason("USER_DELETED");
                row.setDesiredShortVector(null);
                row.setVectorExpiresAt(null);
                return coordinator.requireOwner(lease).then(saveStatus(row, "SKIP_PREPARED"));
            });
        });
    }

    @Override
    public Mono<Void> requireContinuity(VectorLease lease) {
        return coordinator.requireOwner(lease)
                .then(users.exists(lease.userId()))
                .flatMap(exists -> exists ? requireLiveUserContinuity(lease) : Mono.empty())
                .doOnError(ShortTermPreferenceRedisState.ContinuityException.class, error -> log.error(
                        "|PreferenceInteractionRecoveryService|quarantined|userId={}|reason={}",
                        lease.userId(),
                        error.getMessage()));
    }

    private Mono<Void> requireLiveUserContinuity(VectorLease lease) {
        return Mono.zip(
                        shortRedis.cursor(lease.userId()),
                        redis.version(lease.userId()),
                        ledger.findLatestApplied(lease.userId())
                                .map(java.util.Optional::of)
                                .defaultIfEmpty(java.util.Optional.empty()),
                        ledger.findLatestAcknowledged(lease.userId())
                                .map(java.util.Optional::of)
                                .defaultIfEmpty(java.util.Optional.empty()))
                .flatMap(state -> {
                    if (state.getT1().isEmpty()) {
                        return state.getT4().isPresent()
                                ? Mono.error(continuity("Redis lost an acknowledged ST cursor"))
                                : Mono.empty();
                    }
                    ShortTermPreferenceRedisState.Cursor cursor;
                    try {
                        cursor = ShortTermPreferenceRedisState.Cursor.parse(state.getT1());
                    } catch (RuntimeException error) {
                        return Mono.error(continuity("Redis ST cursor is malformed"));
                    }
                    if (state.getT2() < cursor.version()) {
                        return Mono.error(continuity("Redis vector version rolled back behind ST cursor"));
                    }
                    if (state.getT4().isPresent()) {
                        PreferenceInteractionProcessing acknowledged = state.getT4().get();
                        if (!cursor.matches(acknowledged)
                                || cursor.offset() < acknowledged.getCanonicalOffset()
                                || (cursor.offset() == acknowledged.getCanonicalOffset()
                                        && !cursor.operationId().equals(acknowledged.getOperationId()))) {
                            return Mono.error(continuity(
                                    "Redis ST progress rolled back behind durable completed/skipped ledger"));
                        }
                    }
                    if (state.getT3().isPresent()) {
                        PreferenceInteractionProcessing applied = state.getT3().get();
                        if (!cursor.matches(applied)
                                || cursor.offset() < applied.getCanonicalOffset()
                                || (cursor.offset() == applied.getCanonicalOffset()
                                        && !cursor.operationId().equals(applied.getOperationId()))) {
                            return Mono.error(continuity("Redis ST cursor rolled back behind durable applied ledger"));
                        }
                    }
                    // Redis success followed by SQL failure may leave the cursor ahead of SQL's applied status.
                    return ledger.findByOperationId(cursor.operationId())
                            .switchIfEmpty(Mono.error(continuity("Redis ST cursor has no durable operation")))
                            .flatMap(committed -> {
                                if (!lease.userId().equals(committed.getUserId())
                                        || !cursor.matches(committed)
                                        || cursor.offset() != committed.getCanonicalOffset()) {
                                    return Mono.error(continuity("Redis ST cursor identity differs from SQL"));
                                }
                                boolean skipped = "SKIPPED".equals(cursor.mode());
                                if (skipped
                                        ? !List.of("SKIP_PREPARED", "SKIPPED").contains(committed.getStatus())
                                        : !List.of("APPLIED", "EXPIRED").contains(cursor.mode())
                                                || !List.of("PREPARED", "SHORT_APPLIED", "COMPLETED")
                                                        .contains(committed.getStatus())) {
                                    return Mono.error(continuity("Redis ST cursor mode differs from durable operation"));
                                }
                                PreferenceInteractionProcessing snapshot = skipped
                                        ? state.getT3().orElse(null)
                                        : committed;
                                if (snapshot == null
                                        || snapshot.getVectorExpiresAt() == null
                                        || !Instant.now().isBefore(snapshot.getVectorExpiresAt())
                                        || "DESIRED_SNAPSHOT_EXPIRED_BEFORE_COMMIT".equals(snapshot.getReason())
                                        || "EXPIRED".equals(cursor.mode())) {
                                    return Mono.empty();
                                }
                                return Mono.zip(
                                                redis.shortTerm(lease.userId()).defaultIfEmpty(""),
                                                redis.shortTermModel(lease.userId()).defaultIfEmpty(""))
                                        .flatMap(bytes -> {
                                            // Recheck time after I/O: natural TTL expiry during the read is valid.
                                            if (!Instant.now().isBefore(snapshot.getVectorExpiresAt())) {
                                                return Mono.empty();
                                            }
                                            return Objects.equals(bytes.getT1(), snapshot.getDesiredShortVector())
                                                            && VectorMath.MODEL.equals(bytes.getT2())
                                                    ? Mono.empty()
                                                    : Mono.error(continuity(
                                                            "Live acknowledged ST snapshot is missing or changed"));
                                        });
                            });
                });
    }

    private Mono<PreferenceInteractionProcessing> saveStatus(
            PreferenceInteractionProcessing row,
            String status) {
        row.setStatus(status);
        row.setUpdatedAt(micros(Instant.now()));
        return ledger.save(row);
    }

    private Instant micros(Instant value) {
        return value.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    }

    private ShortTermPreferenceRedisState.ContinuityException continuity(String reason) {
        return new ShortTermPreferenceRedisState.ContinuityException(
                reason + "; quarantine user and repair from durable ledger");
    }
}
