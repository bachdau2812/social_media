package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.dauducbach.clone.modules.feed.dto.event.FeedInteractionEvent;
import com.dauducbach.clone.modules.feed.entity.FeedInteractionProcessing;
import com.dauducbach.clone.modules.feed.repositoty.FeedInteractionProcessingRepository;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.service.UserVectorOperationService;
import com.dauducbach.clone.utils.GsonUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FeedInteractionProcessingService {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(FeedInteractionProcessingService.class);
    private final FeedInteractionProcessingRepository ledger;
    private final UserAuditService audit;
    private final FeedVectorService vectors;
    private final UserVectorCoordinator coordinator;
    private final UserVectorOperationService journal;
    private final VectorRedisState redis;
    private final FeedShortTermRedisState shortRedis;
    private final UserDetailsRepository users;

    public record CanonicalPosition(String topic, String generation, int partition, long offset) {
        public CanonicalPosition {
            if (topic == null || topic.isBlank() || generation == null || generation.isBlank() || partition < 0 || offset < 0)
                throw new IllegalArgumentException("Invalid canonical position");
        }
    }

    /** Position is transport metadata, never a synthetic offset derived from source identity. */
    public Mono<Void> apply(FeedInteractionEvent event) {
        return Mono.deferContextual(context -> context.hasKey(CanonicalPosition.class)
                ? apply(event, context.get(CanonicalPosition.class))
                : Mono.error(new IllegalArgumentException("Canonical Kafka position is required")));
    }

    public Mono<Void> apply(FeedInteractionEvent event, CanonicalPosition position) {
        return Mono.defer(() -> loadOrCreate(event, position))
                .flatMap(row -> audit.recordPostInteraction(event).then(auditRecorded(row)))
                // Provider/ranking work never runs under the user lease; this getter reads a built recommendation.
                .flatMap(row -> "AUDIT_RECORDED".equals(row.getStatus())
                        ? users.existsById(event.userId()).flatMap(exists -> exists ? vectors.recommendation(event) : Mono.just(List.<Double>of()))
                            .map(post -> new Input(row, List.copyOf(post)))
                        : Mono.just(new Input(row, List.of())))
                .flatMap(input -> coordinator.withUserLock(event.userId(), lease ->
                        journal.reconcilePending(lease)
                                .then(reconcilePending(lease))
                                .then(requireContinuity(lease))
                                .then(ledger.findById(event.eventId()))
                                .flatMap(row -> processCurrent(event, row, input.post(), lease))))
                .then();
    }

    private record Input(FeedInteractionProcessing row, List<Double> post) {}

    private Mono<FeedInteractionProcessing> loadOrCreate(FeedInteractionEvent event, CanonicalPosition position) {
        return ledger.findById(event.eventId()).switchIfEmpty(Mono.defer(() -> {
            FeedInteractionProcessing row = new FeedInteractionProcessing();
            row.setSourceEventId(event.eventId()); row.setUserId(event.userId()); row.setPostId(event.postId());
            row.setAction(event.action()); row.setSourceId(event.sourceId()); row.setOccurredAt(micros(event.occurredAt()));
            row.setCanonicalTopic(position.topic()); row.setCanonicalGeneration(position.generation());
            row.setCanonicalPartition(position.partition()); row.setCanonicalOffset(position.offset());
            row.setStatus("RECEIVED"); row.setCreatedAt(micros(Instant.now())); row.setUpdatedAt(row.getCreatedAt());
            return ledger.save(row).onErrorResume(DuplicateKeyException.class, ignored -> ledger.findById(event.eventId()));
        })).map(row -> {
            if (!Objects.equals(row.getUserId(), event.userId()) || !Objects.equals(row.getPostId(), event.postId())
                    || !Objects.equals(row.getAction(), event.action()) || !Objects.equals(row.getSourceId(), event.sourceId())
                    || !Objects.equals(row.getOccurredAt(), micros(event.occurredAt())))
                throw new IllegalStateException("Canonical source identity collision; operator investigation required");
            if (!Objects.equals(row.getCanonicalTopic(), position.topic())
                    || !Objects.equals(row.getCanonicalGeneration(), position.generation())
                    || row.getCanonicalPartition() != position.partition())
                throw continuity("Canonical source moved partition/generation without checkpoint migration");
            return row;
        });
    }

    private Mono<FeedInteractionProcessing> auditRecorded(FeedInteractionProcessing row) {
        return "RECEIVED".equals(row.getStatus()) ? saveStatus(row, "AUDIT_RECORDED") : Mono.just(row);
    }

    private Mono<Void> processCurrent(FeedInteractionEvent event, FeedInteractionProcessing row, List<Double> post, VectorLease lease) {
        if (terminal(row)) return Mono.empty();
        if (!"AUDIT_RECORDED".equals(row.getStatus())) return finishPrepared(lease, row);
        return users.existsById(event.userId()).flatMap(exists -> {
            if (!exists || post.isEmpty()) {
                row.setReason(exists ? "POST_DELETED_REJECTED_OR_NO_VALID_INPUT" : "USER_DELETED");
                row.setOperationId(UUID.randomUUID().toString());
                return coordinator.requireOwner(lease).then(saveStatus(row, "SKIP_PREPARED")).flatMap(saved -> finishPrepared(lease, saved));
            }
            return vectors.updateShortTermVector(event, lease, post).flatMap(desired -> {
                row.setOperationId(UUID.randomUUID().toString());
                row.setDesiredShortVector(GsonUtils.getGson().toJson(desired));
                row.setVectorExpiresAt(Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS));
                return coordinator.requireOwner(lease).then(saveStatus(row, "PREPARED"))
                        .flatMap(saved -> finishPrepared(lease, saved));
            });
        });
    }

    /** Call only under the common lease. Recovery finishes SQL's desired snapshot; it never reblends. */
    public Mono<Void> reconcilePending(VectorLease lease) {
        return requireContinuity(lease).thenMany(ledger.findPending(lease.userId()))
                .concatMap(row -> finishPrepared(lease, row), 1).then().then(requireContinuity(lease));
    }

    private Mono<Void> finishPrepared(VectorLease lease, FeedInteractionProcessing row) {
        if ("SHORT_APPLIED".equals(row.getStatus())) return requireContinuity(lease).then(saveStatus(row, "COMPLETED")).then();
        if (!"PREPARED".equals(row.getStatus()) && !"SKIP_PREPARED".equals(row.getStatus()))
            return Mono.error(new IllegalStateException("Unexpected processing state: " + row.getStatus()));
        return prepareDeletedSkip(lease, row).flatMap(prepared -> Mono.zip(shortRedis.cursor(lease.userId()), redis.version(lease.userId()))
                .flatMap(state -> users.existsById(lease.userId()).flatMap(exists -> {
                    // User deletion deliberately clears ST/version; never restore an already applied snapshot.
                    if (!exists && !state.getT1().isEmpty()
                            && FeedShortTermRedisState.Cursor.parse(state.getT1()).operationId().equals(prepared.getOperationId()))
                        return coordinator.requireOwner(lease).thenReturn(state.getT2());
                    return shortRedis.commit(lease, prepared, state.getT1(), state.getT2());
                })))
                .then(shortRedis.cursor(lease.userId()))
                .flatMap(value -> {
                    FeedShortTermRedisState.Cursor cursor = FeedShortTermRedisState.Cursor.parse(value);
                    if (!cursor.matches(row) || !cursor.operationId().equals(row.getOperationId()))
                        return Mono.error(continuity("Committed ST cursor differs from prepared operation"));
                    if ("SKIP_PREPARED".equals(row.getStatus())) return coordinator.requireOwner(lease).then(saveStatus(row, "SKIPPED"));
                    if ("EXPIRED".equals(cursor.mode())) row.setReason("DESIRED_SNAPSHOT_EXPIRED_BEFORE_COMMIT");
                    return coordinator.requireOwner(lease).then(saveStatus(row, "SHORT_APPLIED"))
                            .flatMap(saved -> coordinator.requireOwner(lease).then(saveStatus(saved, "COMPLETED")));
                }).then();
    }

    private Mono<FeedInteractionProcessing> prepareDeletedSkip(VectorLease lease, FeedInteractionProcessing row) {
        if (!"PREPARED".equals(row.getStatus())) return Mono.just(row);
        return users.existsById(lease.userId()).flatMap(exists -> {
            if (exists) return Mono.just(row);
            return shortRedis.cursor(lease.userId()).flatMap(value -> {
                // Already acknowledged before deletion: finish its durable state without writing it again.
                if (!value.isEmpty() && FeedShortTermRedisState.Cursor.parse(value).operationId().equals(row.getOperationId()))
                    return Mono.just(row);
                row.setReason("USER_DELETED"); row.setDesiredShortVector(null); row.setVectorExpiresAt(null);
                return coordinator.requireOwner(lease).then(saveStatus(row, "SKIP_PREPARED"));
            });
        });
    }

    /** Task9 can call this under its lease to stop on acknowledged snapshot rollback before replay. */
    public Mono<Void> requireContinuity(VectorLease lease) {
        return coordinator.requireOwner(lease).then(users.existsById(lease.userId()))
                .flatMap(exists -> exists ? requireLiveUserContinuity(lease) : Mono.empty())
                .doOnError(VectorRepairRequiredException.class, error -> log.error(
                        "|FeedInteractionProcessingService|quarantined|userId={}|reason={}", lease.userId(), error.getMessage()));
    }

    private Mono<Void> requireLiveUserContinuity(VectorLease lease) {
        return Mono.zip(shortRedis.cursor(lease.userId()), redis.version(lease.userId()),
                        ledger.findLatestApplied(lease.userId()).map(java.util.Optional::of).defaultIfEmpty(java.util.Optional.empty()),
                        ledger.findLatestAcknowledged(lease.userId()).map(java.util.Optional::of).defaultIfEmpty(java.util.Optional.empty()))
                .flatMap(state -> {
                    if (state.getT1().isEmpty()) return state.getT4().isPresent()
                            ? Mono.error(continuity("Redis lost an acknowledged ST cursor")) : Mono.empty();
                    FeedShortTermRedisState.Cursor cursor;
                    try { cursor = FeedShortTermRedisState.Cursor.parse(state.getT1()); }
                    catch (RuntimeException error) { return Mono.error(continuity("Redis ST cursor is malformed")); }
                    if (state.getT2() < cursor.version()) return Mono.error(continuity("Redis vector version rolled back behind ST cursor"));
                    if (state.getT4().isPresent()) {
                        FeedInteractionProcessing acknowledged = state.getT4().get();
                        if (!cursor.matches(acknowledged) || cursor.offset() < acknowledged.getCanonicalOffset()
                                || (cursor.offset() == acknowledged.getCanonicalOffset() && !cursor.operationId().equals(acknowledged.getOperationId())))
                            return Mono.error(continuity("Redis ST progress rolled back behind durable completed/skipped ledger"));
                    }
                    if (state.getT3().isPresent()) {
                        FeedInteractionProcessing applied = state.getT3().get();
                        if (!cursor.matches(applied) || cursor.offset() < applied.getCanonicalOffset()
                                || (cursor.offset() == applied.getCanonicalOffset() && !cursor.operationId().equals(applied.getOperationId())))
                            return Mono.error(continuity("Redis ST cursor rolled back behind durable applied ledger"));
                    }
                    // A Redis-success/SQL-failure window can put the cursor ahead of SQL's applied status.
                    return ledger.findByOperationId(cursor.operationId())
                            .switchIfEmpty(Mono.error(continuity("Redis ST cursor has no durable operation")))
                            .flatMap(committed -> {
                                if (!lease.userId().equals(committed.getUserId()) || !cursor.matches(committed) || cursor.offset() != committed.getCanonicalOffset())
                                    return Mono.error(continuity("Redis ST cursor identity differs from SQL"));
                                boolean skipped = "SKIPPED".equals(cursor.mode());
                                if (skipped ? !List.of("SKIP_PREPARED", "SKIPPED").contains(committed.getStatus())
                                        : !List.of("APPLIED", "EXPIRED").contains(cursor.mode())
                                            || !List.of("PREPARED", "SHORT_APPLIED", "COMPLETED").contains(committed.getStatus()))
                                    return Mono.error(continuity("Redis ST cursor mode differs from durable operation"));
                                FeedInteractionProcessing snapshot = "SKIPPED".equals(cursor.mode())
                                        ? state.getT3().orElse(null) : committed;
                                if (snapshot == null || snapshot.getVectorExpiresAt() == null
                                        || !Instant.now().isBefore(snapshot.getVectorExpiresAt())
                                        || "DESIRED_SNAPSHOT_EXPIRED_BEFORE_COMMIT".equals(snapshot.getReason())
                                        || "EXPIRED".equals(cursor.mode())) return Mono.empty();
                                return Mono.zip(redis.shortTerm(lease.userId()).defaultIfEmpty(""),
                                                redis.shortTermModel(lease.userId()).defaultIfEmpty(""))
                                        .flatMap(bytes -> {
                                            // Recheck time after I/O: natural TTL expiry during the read is valid.
                                            if (!Instant.now().isBefore(snapshot.getVectorExpiresAt())) return Mono.empty();
                                            return Objects.equals(bytes.getT1(), snapshot.getDesiredShortVector())
                                                    && VectorMath.MODEL.equals(bytes.getT2()) ? Mono.empty()
                                                    : Mono.error(continuity("Live acknowledged ST snapshot is missing or changed"));
                                        });
                            });
                });
    }

    private Mono<FeedInteractionProcessing> saveStatus(FeedInteractionProcessing row, String status) {
        row.setStatus(status); row.setUpdatedAt(micros(Instant.now()));
        return ledger.save(row);
    }
    private boolean terminal(FeedInteractionProcessing row) { return "COMPLETED".equals(row.getStatus()) || "SKIPPED".equals(row.getStatus()); }
    private Instant micros(Instant value) { return value.truncatedTo(ChronoUnit.MICROS); }
    private FeedShortTermRedisState.ContinuityException continuity(String reason) {
        return new FeedShortTermRedisState.ContinuityException(reason + "; quarantine user and repair from durable ledger");
    }
}
