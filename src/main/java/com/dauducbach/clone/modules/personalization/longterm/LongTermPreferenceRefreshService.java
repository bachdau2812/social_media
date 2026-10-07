package com.dauducbach.clone.modules.personalization.longterm;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease;
import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryEntry;
import com.dauducbach.clone.modules.audit.publicapi.InteractionHistoryQuery;
import com.dauducbach.clone.modules.post.publicapi.PostFeedQuery;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.model.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.personalization.interactions.InteractionWeightPolicy;
import com.dauducbach.clone.modules.personalization.publicapi.LongTermPreferenceRefresh;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

@Service
@RequiredArgsConstructor
public class LongTermPreferenceRefreshService implements LongTermPreferenceRefresh {
    private static final Logger log = LoggerFactory.getLogger(LongTermPreferenceRefreshService.class);
    private static final double LONG_TERM_ALPHA = 0.7d;
    private static final String FORMULA = "long-term-alpha-0.7-repost-v1";
    private static final ZoneId SCHEDULE_ZONE = ZoneId.of("Asia/Ho_Chi_Minh");

    private final InteractionHistoryQuery auditInteractionQueryService;
    private final PostFeedQuery postFeedQueryService;
    private final UserVectorQueryService userVectorQueryService;
    private final UserExistenceQuery users;
    private final UserVectorCoordinator coordinator;
    private final PreferenceRecoveryCoordinator recovery;
    private final UserVectorOperationService operations;
    private final InteractionWeightPolicy weightPolicy;

    @Scheduled(cron = "0 0 2 * * *", zone = "Asia/Ho_Chi_Minh")
    public Mono<Void> updateYesterdayLongTermVectors() {
        return Mono.defer(() -> {
            TimeRange range = yesterdayRange();
            return updateLongTermVectorsForRange(range.from(), range.to())
                    .doOnError(error -> log.error(
                            "|LongTermPreferenceRefreshService|updateYesterdayLongTermVectors|failed|from={}|to={}",
                            range.from(), range.to(), error));
        });
    }

    @Override
    public Mono<PreferenceRefreshResult> refreshLongTermVectors(String from, String to, String userId) {
        TimeRange range = resolveRefreshRange(from, to);
        String cleanUserId = userId == null ? "" : userId.trim();
        Mono<String> refresh = cleanUserId.isBlank()
                ? refreshRange(range.from(), range.to())
                : refreshUserRange(cleanUserId, range.from(), range.to());
        return refresh.map(status -> new PreferenceRefreshResult(cleanUserId.isBlank() ? null : cleanUserId,
                        range.from(), range.to(), Instant.now(), status))
                .onErrorMap(error -> error instanceof AppException ? error : new AppException(
                        ErrorCode.FEED_LONG_TERM_VECTOR_REFRESH_FAILED,
                        String.format("Refresh feed long term vectors failed from=%s to=%s", range.from(), range.to()), error));
    }

    public Mono<Void> updateLongTermVectorsForRange(Instant from, Instant to) {
        return refreshRange(from, to).then();
    }

    public Mono<Void> updateLongTermVectorForUserInRange(String userId, Instant from, Instant to) {
        return userId == null || userId.isBlank() ? updateLongTermVectorsForRange(from, to)
                : refreshUserRange(userId.trim(), from, to).then();
    }

    private Mono<String> refreshRange(Instant from, Instant to) {
        return auditInteractionQueryService.findPostInteractionsBetween(from, to)
                .filter(audit -> audit.actorId() != null && !audit.actorId().isBlank())
                .collectMultimap(InteractionHistoryEntry::actorId)
                .flatMapMany(map -> Flux.fromIterable(map.entrySet()))
                .concatMap(entry -> updateUserLongTermVector(entry.getKey(), entry.getValue(), from, to))
                .collectList().map(this::rangeStatus);
    }

    private String rangeStatus(List<String> results) {
        if (results.isEmpty()) return "SKIPPED_NO_SIGNAL";
        if (results.stream().allMatch("ALREADY_APPLIED"::equals)) return "ALREADY_APPLIED";
        if (results.size() == 1) return results.getFirst();
        long skipped = results.stream().filter(result -> !"COMPLETED".equals(result)).count();
        return skipped == 0 ? "COMPLETED" : "COMPLETED_WITH_SKIPPED:" + skipped;
    }

    private Mono<String> refreshUserRange(String userId, Instant from, Instant to) {
        return auditInteractionQueryService.findPostInteractionsBetween(from, to)
                .filter(audit -> userId.equals(audit.actorId())).collectList()
                .flatMap(audits -> updateUserLongTermVector(userId, audits, from, to));
    }

    private Mono<String> updateUserLongTermVector(String userId, Collection<InteractionHistoryEntry> audits, Instant from, Instant to) {
        String key = "long-term:" + fingerprint(userId + "|" + from + "|" + to + "|" + FORMULA);
        // The journal's range columns have microsecond precision. The key retains exact API instants.
        Instant journalFrom = from.truncatedTo(ChronoUnit.MICROS);
        Instant journalTo = to.truncatedTo(ChronoUnit.MICROS);
        return Mono.defer(() -> users.exists(userId).flatMap(exists -> exists
                        ? operations.findByOperationKey(key).map(Optional::of).defaultIfEmpty(Optional.empty())
                            .flatMap(hint -> hint.isPresent() && !"CONFLICTED".equals(hint.get().getStatus())
                                    // Existing desired state is recovered without requiring today's posts again.
                                    ? Mono.just(new TodayInput(List.of(), false))
                                    : calculateTodayVector(audits).map(vector -> new TodayInput(vector, true)))
                        : Mono.just(new TodayInput(List.of(), false)))
                .flatMap(today -> coordinator.withUserLock(userId, lease -> users.exists(userId).flatMap(exists -> {
                    if (!exists) return operations.cancelForDeleted(lease).thenReturn("SKIPPED_USER_DELETED");
                    return recovery.reconcilePending(lease)
                            .then(operations.findByOperationKey(key).map(Optional::of).defaultIfEmpty(Optional.empty()))
                            .flatMap(checkpoint -> {
                                if (checkpoint.isPresent() && "COMPLETED".equals(checkpoint.get().getStatus()))
                                    return Mono.just("ALREADY_APPLIED");
                                // A previous owner may have conflicted the outside-lock hint before acquisition.
                                // Release the lease and prepare again; skipped preparation is never a no-signal batch.
                                if (!today.calculated()) return Mono.error(new TodayPreparationRequiredException());
                                return applyPreparedToday(lease, key, today.vector(), journalFrom, journalTo);
                            });
                }))))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(100))
                        .filter(error -> error instanceof UserVectorStore.RetryableVectorConflictException
                                || error instanceof TodayPreparationRequiredException
                                || error instanceof TimeoutException || error instanceof UserVectorCoordinator.LeaseLostException)
                        .onRetryExhaustedThrow((spec, signal) -> signal.failure()))
                .doOnSuccess(status -> log.info("|LongTermPreferenceRefreshService|range|userId={}|from={}|to={}|status={}",
                        userId, from, to, status))
                .doOnError(error -> log.error("|LongTermPreferenceRefreshService|range|failed|userId={}|from={}|to={}",
                        userId, from, to, error));
    }

    private Mono<String> applyPreparedToday(VectorLease lease, String key, List<Double> today, Instant from, Instant to) {
        if (today.isEmpty()) return operations.checkpoint(lease, key, FORMULA, from, to)
                .map(operation -> "SKIPPED_NO_SIGNAL");
        // Reconciliation precedes this fresh ES read; the exact captured OCC snapshot accompanies the blend.
        return userVectorQueryService.getSnapshot(lease.userId())
                .switchIfEmpty(Mono.error(new IllegalStateException("Missing user vector document; profile repair required")))
                .flatMap(captured -> {
                    List<Double> old = userVectorQueryService.compatibleLongTermBase(captured);
                    List<Double> desired = old.isEmpty() ? today
                            : VectorMath.mix(old, LONG_TERM_ALPHA, today, 1 - LONG_TERM_ALPHA);
                    return operations.applyLongTerm(lease, key, captured, desired, FORMULA, from, to)
                            .map(operation -> "COMPLETED");
                });
    }

    private Mono<List<Double>> calculateTodayVector(Collection<InteractionHistoryEntry> audits) {
        return Flux.fromIterable(audits == null ? List.<InteractionHistoryEntry>of() : audits)
                .filter(audit -> "SUCCESS".equals(audit.status()) && supportedAction(audit.action()))
                // Deduplicate source identity, never actor/post: repeated genuine interactions still contribute.
                .distinct(this::auditIdentity)
                .concatMap(audit -> {
                    String postId = resolvePostId(audit);
                    double weight = weightPolicy.longWeight(audit.action().name());
                    if (postId.isBlank() || weight == 0) return Mono.just(List.<Double>of());
                    return postFeedQueryService.getRecommendationVector(postId)
                            .map(vector -> vector.isEmpty() ? vector : multiplyVector(VectorMath.normalize(vector), weight));
                })
                .filter(vector -> !vector.isEmpty()).reduce(this::sumVectors)
                .map(sum -> sum.stream().allMatch(value -> value == 0d) ? List.<Double>of() : VectorMath.normalize(sum))
                .defaultIfEmpty(List.of());
    }

    private boolean supportedAction(AuditActionType action) {
        return action == AuditActionType.LIKE_POST || action == AuditActionType.COMMENT_POST || action == AuditActionType.REPOST_POST;
    }

    private String auditIdentity(InteractionHistoryEntry audit) {
        if (audit.sourceEventId() != null && !audit.sourceEventId().isBlank()) return "source:" + audit.sourceEventId();
        String sourceId = metadataText(audit, "sourceId");
        if (!sourceId.isBlank()) return "legacy-source:" + audit.actorId() + "|" + audit.action() + "|" + sourceId;
        if (audit.auditId() != null && !audit.auditId().isBlank()) return "audit:" + audit.auditId();
        return "legacy:" + audit.actorId() + "|" + audit.action() + "|" + audit.resourceId()
                + "|" + audit.createdAt() + "|" + audit.metadata();
    }

    private String resolvePostId(InteractionHistoryEntry audit) {
        String postId = metadataText(audit, "postId");
        if (!postId.isBlank()) return postId;
        if (audit.action() == AuditActionType.LIKE_POST || audit.action() == AuditActionType.REPOST_POST)
            return audit.resourceId() == null ? "" : audit.resourceId().trim();
        return "";
    }

    private String metadataText(InteractionHistoryEntry audit, String key) {
        Object value = audit.metadata().get(key);
        return value == null ? "" : value.toString().trim();
    }

    private List<Double> multiplyVector(List<Double> vector, double weight) {
        return vector.stream().map(value -> value * weight).toList();
    }

    private List<Double> sumVectors(List<Double> left, List<Double> right) {
        List<Double> sum = new ArrayList<>(VectorMath.DIMENSION);
        for (int index = 0; index < VectorMath.DIMENSION; index++) sum.add(left.get(index) + right.get(index));
        return sum;
    }

    private String fingerprint(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) { throw new IllegalStateException(error); }
    }

    private TimeRange resolveRefreshRange(String from, String to) {
        boolean hasFrom = from != null && !from.isBlank();
        boolean hasTo = to != null && !to.isBlank();
        if (!hasFrom && !hasTo) return yesterdayRange();
        if (!hasFrom || !hasTo)
            throw new AppException(ErrorCode.FEED_REQUEST_INVALID, "Both from and to are required when refreshing a custom range");
        try {
            Instant fromInstant = Instant.parse(from.trim());
            Instant toInstant = Instant.parse(to.trim());
            if (!fromInstant.isBefore(toInstant)) throw new AppException(ErrorCode.FEED_REQUEST_INVALID, "from must be before to");
            return new TimeRange(fromInstant, toInstant);
        } catch (DateTimeException error) {
            throw new AppException(ErrorCode.FEED_REQUEST_INVALID, "from and to must be ISO-8601 instants", error);
        }
    }

    private TimeRange yesterdayRange() {
        LocalDate yesterday = LocalDate.now(SCHEDULE_ZONE).minusDays(1);
        return new TimeRange(yesterday.atStartOfDay(SCHEDULE_ZONE).toInstant(),
                yesterday.plusDays(1).atStartOfDay(SCHEDULE_ZONE).toInstant());
    }

    private record TodayInput(List<Double> vector, boolean calculated) { }
    private static class TodayPreparationRequiredException extends RuntimeException {
        private TodayPreparationRequiredException() { super("Range hint changed; prepare today's direction outside the user lease"); }
    }
    private record TimeRange(Instant from, Instant to) { }
}
