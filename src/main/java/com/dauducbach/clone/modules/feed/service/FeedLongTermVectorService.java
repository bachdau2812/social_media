package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.audit.dto.AuditActionType;
import com.dauducbach.clone.modules.audit.entity.AuditLogs;
import com.dauducbach.clone.modules.audit.service.AuditInteractionQueryService;
import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.feed.dto.response.FeedLongTermVectorRefreshResponse;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import com.dauducbach.clone.modules.user.service.UserVectorQueryService;
import com.dauducbach.clone.utils.GsonUtils;
import com.dauducbach.clone.utils.KafkaUtils;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedLongTermVectorService {
    private static final Logger log = LoggerFactory.getLogger(FeedLongTermVectorService.class);
    private static final double LONG_TERM_ALPHA = 0.7d;
    private static final double LIKE_WEIGHT = 0.2d;
    private static final double COMMENT_WEIGHT = 0.3d;
    private static final Duration LONG_TERM_SNAPSHOT_TTL = Duration.ofHours(6);
    private static final ZoneId SCHEDULE_ZONE = ZoneId.of("Asia/Ho_Chi_Minh");
    private static final Type DOUBLE_LIST_TYPE = new TypeToken<List<Double>>() {
    }.getType();

    AuditInteractionQueryService auditInteractionQueryService;
    PostFeedQueryService postFeedQueryService;
    UserVectorQueryService userVectorQueryService;
    UserDetailsService userDetailsService;
    FeedVectorService feedVectorService;
    ReactiveRedisTemplate<String, String> redisTemplate;

    @Scheduled(cron = "0 0 2 * * *", zone = "Asia/Ho_Chi_Minh")
    public void updateYesterdayLongTermVectors() {
        TimeRange range = yesterdayRange();
        updateLongTermVectorsForRange(range.from(), range.to())
                .doOnError(error -> log.error("|FeedLongTermVectorService|updateYesterdayLongTermVectors|failed|error={}",
                        error.getMessage()))
                .subscribe();
    }

    public Mono<FeedLongTermVectorRefreshResponse> refreshLongTermVectors(String from, String to, String userId) {
        TimeRange range = resolveRefreshRange(from, to);
        String cleanUserId = normalizeUserId(userId);
        log.info("|FeedLongTermVectorService|refreshLongTermVectors|start|userId={}|from={}|to={}",
                cleanUserId, range.from(), range.to());

        Mono<Void> refresh = cleanUserId.isBlank()
                ? updateLongTermVectorsForRange(range.from(), range.to())
                : validateUserExists(cleanUserId)
                        .then(updateLongTermVectorForUserInRange(cleanUserId, range.from(), range.to()));

        return refresh
                .thenReturn(new FeedLongTermVectorRefreshResponse(cleanUserId.isBlank() ? null : cleanUserId,
                        range.from(), range.to(), Instant.now(), "COMPLETED"))
                .doOnSuccess(response -> log.info("|FeedLongTermVectorService|refreshLongTermVectors|completed|from={}|to={}",
                        response.from(), response.to()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.FEED_LONG_TERM_VECTOR_REFRESH_FAILED,
                                String.format("Refresh feed long term vectors failed from=%s to=%s", range.from(), range.to()),
                                error
                        ));
    }

    public Mono<Void> updateLongTermVectorsForRange(Instant from, Instant to) {
        return auditInteractionQueryService.findPostInteractionsBetween(from, to)
                .filter(log -> log.getActorId() != null && !log.getActorId().isBlank())
                .collectMultimap(AuditLogs::getActorId)
                .flatMapMany(map -> Flux.fromIterable(map.entrySet()))
                .concatMap(entry -> updateUserLongTermVector(entry.getKey(), entry.getValue()))
                .then()
                .doOnSuccess(unused -> log.info("|FeedLongTermVectorService|updateLongTermVectorsForRange|completed|from={}|to={}",
                        from, to));
    }

    public Mono<Void> updateLongTermVectorForUserInRange(String userId, Instant from, Instant to) {
        if (userId == null || userId.isBlank()) {
            return updateLongTermVectorsForRange(from, to);
        }

        String cleanUserId = userId.trim();
        return auditInteractionQueryService.findPostInteractionsBetween(from, to)
                .filter(auditLog -> cleanUserId.equals(auditLog.getActorId()))
                .collectList()
                .flatMap(logs -> updateUserLongTermVector(cleanUserId, logs))
                .doOnSuccess(unused -> log.info("|FeedLongTermVectorService|updateLongTermVectorForUserInRange|completed|userId={}|from={}|to={}",
                        cleanUserId, from, to));
    }

    private TimeRange resolveRefreshRange(String from, String to) {
        boolean hasFrom = from != null && !from.isBlank();
        boolean hasTo = to != null && !to.isBlank();
        if (!hasFrom && !hasTo) {
            return yesterdayRange();
        }
        if (!hasFrom || !hasTo) {
            throw new AppException(ErrorCode.FEED_REQUEST_INVALID, "Both from and to are required when refreshing a custom range");
        }

        try {
            Instant fromInstant = Instant.parse(from.trim());
            Instant toInstant = Instant.parse(to.trim());
            if (!fromInstant.isBefore(toInstant)) {
                throw new AppException(ErrorCode.FEED_REQUEST_INVALID, "from must be before to");
            }
            return new TimeRange(fromInstant, toInstant);
        } catch (DateTimeException error) {
            throw new AppException(ErrorCode.FEED_REQUEST_INVALID, "from and to must be ISO-8601 instants", error);
        }
    }

    private TimeRange yesterdayRange() {
        LocalDate yesterday = LocalDate.now(SCHEDULE_ZONE).minusDays(1);
        Instant from = yesterday.atStartOfDay(SCHEDULE_ZONE).toInstant();
        Instant to = yesterday.plusDays(1).atStartOfDay(SCHEDULE_ZONE).toInstant();
        return new TimeRange(from, to);
    }

    private Mono<Void> validateUserExists(String userId) {
        return userDetailsService.getUserDetailsById(userId)
                .then();
    }

    private String normalizeUserId(String userId) {
        return userId == null ? "" : userId.trim();
    }

    private Mono<Void> updateUserLongTermVector(String userId, Collection<AuditLogs> logs) {
        return Mono.zip(loadAndSnapshotOldVector(userId), calculateTodayVector(logs))
                .flatMap(tuple -> {
                    List<Double> oldVector = tuple.getT1();
                    List<Double> todayVector = tuple.getT2();
                    if (todayVector.isEmpty()) {
                        return clearSnapshot(userId);
                    }

                    List<Double> updatedVector = blendLongTermVector(oldVector, todayVector);
                    return userVectorQueryService.saveLongTermVector(userId, updatedVector)
                            .then(clearSnapshot(userId));
                })
                .doOnSuccess(unused -> log.info("|FeedLongTermVectorService|updateUserLongTermVector|userId={}|interactionCount={}",
                        userId, logs.size()))
                .onErrorResume(error -> {
                    log.error("|FeedLongTermVectorService|updateUserLongTermVector|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return clearSnapshot(userId);
                });
    }

    private Mono<List<Double>> loadAndSnapshotOldVector(String userId) {
        String snapshotKey = FeedCacheKeys.userLongTermVectorSnapshot(userId);
        return redisTemplate.opsForValue()
                .get(snapshotKey)
                .map(this::parseVector)
                .filter(vector -> !vector.isEmpty())
                .switchIfEmpty(userVectorQueryService.getLongTermOrUserVector(userId)
                        .flatMap(vector -> vector.isEmpty()
                                ? Mono.just(vector)
                                : redisTemplate.opsForValue()
                                        .set(snapshotKey, GsonUtils.getGson().toJson(vector), LONG_TERM_SNAPSHOT_TTL)
                                        .thenReturn(vector)))
                .onErrorResume(error -> {
                    log.warn("|FeedLongTermVectorService|loadAndSnapshotOldVector|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return userVectorQueryService.getLongTermOrUserVector(userId);
                });
    }

    private Mono<List<Double>> calculateTodayVector(Collection<AuditLogs> logs) {
        return Flux.fromIterable(logs == null ? List.<AuditLogs>of() : logs)
                .concatMap(log -> {
                    String postId = resolvePostId(log);
                    if (postId.isBlank()) {
                        return Mono.just(List.<Double>of());
                    }
                    double weight = resolveActionWeight(log.getAction());
                    return postFeedQueryService.getPostVector(postId)
                            .map(vector -> multiplyVector(vector, weight));
                })
                .filter(vector -> !vector.isEmpty())
                .reduce(this::sumVectors)
                .map(feedVectorService::normalize)
                .defaultIfEmpty(List.of());
    }

    private List<Double> blendLongTermVector(List<Double> oldVector, List<Double> todayVector) {
        if (oldVector == null || oldVector.isEmpty()) {
            return feedVectorService.normalize(todayVector);
        }

        int size = Math.max(oldVector.size(), todayVector.size());
        List<Double> blended = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            blended.add(LONG_TERM_ALPHA * valueAt(oldVector, i) + (1 - LONG_TERM_ALPHA) * valueAt(todayVector, i));
        }
        return feedVectorService.normalize(blended);
    }

    private String resolvePostId(AuditLogs auditLog) {
        if (AuditActionType.LIKE_POST.equals(auditLog.getAction())) {
            return auditLog.getResourceId() == null ? "" : auditLog.getResourceId();
        }

        JsonObject metadata = GsonUtils.fromString(auditLog.getMetadata());
        return KafkaUtils.extractString(metadata, "postId");
    }

    private double resolveActionWeight(AuditActionType action) {
        if (AuditActionType.COMMENT_POST.equals(action)) {
            return COMMENT_WEIGHT;
        }
        return LIKE_WEIGHT;
    }

    private List<Double> multiplyVector(List<Double> vector, double weight) {
        if (vector == null || vector.isEmpty()) {
            return List.of();
        }
        return vector.stream().map(value -> (value == null ? 0d : value) * weight).toList();
    }

    private List<Double> sumVectors(List<Double> left, List<Double> right) {
        int size = Math.max(left.size(), right.size());
        List<Double> sum = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            sum.add(valueAt(left, i) + valueAt(right, i));
        }
        return sum;
    }

    private Mono<Void> clearSnapshot(String userId) {
        return redisTemplate.delete(FeedCacheKeys.userLongTermVectorSnapshot(userId)).then();
    }

    private List<Double> parseVector(String json) {
        try {
            List<Double> parsed = GsonUtils.getGson().fromJson(json, DOUBLE_LIST_TYPE);
            return parsed == null ? List.of() : parsed;
        } catch (Exception error) {
            return List.of();
        }
    }

    private double valueAt(List<Double> vector, int index) {
        if (vector == null || index < 0 || index >= vector.size() || vector.get(index) == null) {
            return 0d;
        }
        return vector.get(index);
    }

    private record TimeRange(Instant from, Instant to) {
    }
}
