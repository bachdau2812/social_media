package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.modules.user.service.UserVectorQueryService;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedVectorService {
    private static final Logger log = LoggerFactory.getLogger(FeedVectorService.class);
    private static final double SHORT_TERM_DECAY = 0.7d;
    private static final double LIKE_WEIGHT = 0.3d;
    private static final double COMMENT_WEIGHT = 0.7d;
    private static final double USER_WITH_HISTORY_LONG_WEIGHT = 0.7d;
    private static final double USER_WITH_HISTORY_SHORT_WEIGHT = 0.3d;
    private static final double NEW_USER_LONG_WEIGHT = 0.3d;
    private static final double NEW_USER_SHORT_WEIGHT = 0.7d;
    private static final Duration SHORT_TERM_VECTOR_TTL = Duration.ofDays(1);
    private static final Type DOUBLE_LIST_TYPE = new TypeToken<List<Double>>() {
    }.getType();

    ReactiveRedisTemplate<String, String> redisTemplate;
    PostFeedQueryService postFeedQueryService;
    UserVectorQueryService userVectorQueryService;

    public Mono<Void> updateShortTermVector(String userId, String postId, String action) {
        if (userId == null || userId.isBlank() || postId == null || postId.isBlank()) {
            return Mono.empty();
        }

        double weight = resolveActionWeight(action);
        if (weight <= 0) {
            log.warn("|FeedVectorService|updateShortTermVector|skip unsupported action|userId={}|postId={}|action={}",
                    userId, postId, action);
            return Mono.empty();
        }

        return Mono.zip(getShortTermVector(userId), postFeedQueryService.getPostVector(postId))
                .map(tuple -> calculateShortTermVector(tuple.getT1(), tuple.getT2(), weight))
                .filter(vector -> !vector.isEmpty())
                .flatMap(vector -> redisTemplate.opsForValue()
                        .set(FeedCacheKeys.userShortTermVector(userId), GsonUtils.getGson().toJson(vector), SHORT_TERM_VECTOR_TTL)
                        .then())
                .doOnSuccess(unused -> log.info("|FeedVectorService|updateShortTermVector|success|userId={}|postId={}|action={}",
                        userId, postId, action))
                .onErrorResume(error -> {
                    log.warn("|FeedVectorService|updateShortTermVector|failed|userId={}|postId={}|error={}",
                            userId, postId, error.getMessage());
                    return Mono.empty();
                });
    }

    public Mono<List<Double>> buildQueryVector(String userId) {
        return Mono.zip(
                        userVectorQueryService.getLongTermVector(userId),
                        getShortTermVector(userId),
                        userVectorQueryService.getUserVector(userId)
                )
                .map(tuple -> combineWithUserVectorFallback(tuple.getT1(), tuple.getT2(), tuple.getT3()))
                .onErrorResume(error -> {
                    log.warn("|FeedVectorService|buildQueryVector|failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    public Mono<List<Double>> getShortTermVector(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(List.of());
        }

        return redisTemplate.opsForValue()
                .get(FeedCacheKeys.userShortTermVector(userId))
                .map(this::parseVector)
                .defaultIfEmpty(List.of())
                .onErrorResume(error -> {
                    log.warn("|FeedVectorService|getShortTermVector|failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    List<Double> calculateShortTermVector(List<Double> oldVector, List<Double> postVector, double weight) {
        if (postVector == null || postVector.isEmpty()) {
            return List.of();
        }

        int size = Math.max(safeSize(oldVector), postVector.size());
        List<Double> combined = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            double oldValue = valueAt(oldVector, i);
            double postValue = valueAt(postVector, i);
            combined.add(SHORT_TERM_DECAY * oldValue + weight * postValue);
        }
        return normalize(combined);
    }

    List<Double> combineLongAndShortTerm(List<Double> longTermVector, List<Double> shortTermVector) {
        boolean hasLongTerm = longTermVector != null && !longTermVector.isEmpty();
        boolean hasShortTerm = shortTermVector != null && !shortTermVector.isEmpty();

        if (!hasLongTerm && !hasShortTerm) {
            return List.of();
        }
        if (!hasLongTerm) {
            return normalize(shortTermVector);
        }
        if (!hasShortTerm) {
            return normalize(longTermVector);
        }

        double longWeight = USER_WITH_HISTORY_LONG_WEIGHT;
        double shortWeight = USER_WITH_HISTORY_SHORT_WEIGHT;
        if (longTermVector.size() < shortTermVector.size()) {
            longWeight = NEW_USER_LONG_WEIGHT;
            shortWeight = NEW_USER_SHORT_WEIGHT;
        }

        int size = Math.max(longTermVector.size(), shortTermVector.size());
        List<Double> combined = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            combined.add(longWeight * valueAt(longTermVector, i) + shortWeight * valueAt(shortTermVector, i));
        }
        return normalize(combined);
    }

    List<Double> combineWithUserVectorFallback(List<Double> longTermVector,
                                               List<Double> shortTermVector,
                                               List<Double> userVector) {
        List<Double> safeUserVector = userVector == null ? List.of() : userVector;
        List<Double> longCandidate = longTermVector == null || longTermVector.isEmpty()
                ? safeUserVector
                : longTermVector;
        List<Double> shortCandidate = shortTermVector == null || shortTermVector.isEmpty()
                ? safeUserVector
                : shortTermVector;

        return combineLongAndShortTerm(longCandidate, shortCandidate);
    }

    List<Double> normalize(List<Double> vector) {
        if (vector == null || vector.isEmpty()) {
            return List.of();
        }

        double sumOfSquares = vector.stream()
                .mapToDouble(value -> value == null ? 0d : value * value)
                .sum();
        double length = Math.sqrt(sumOfSquares);
        if (length == 0d) {
            return List.of();
        }

        return vector.stream()
                .map(value -> value == null ? 0d : value / length)
                .toList();
    }

    private List<Double> parseVector(String json) {
        try {
            List<Double> parsed = GsonUtils.getGson().fromJson(json, DOUBLE_LIST_TYPE);
            return parsed == null ? List.of() : parsed;
        } catch (Exception error) {
            log.warn("|FeedVectorService|parseVector|failed|error={}", error.getMessage());
            return List.of();
        }
    }

    private double resolveActionWeight(String action) {
        if (action == null) {
            return 0d;
        }

        return switch (action.trim().toUpperCase()) {
            case "LIKE", "LIKE_POST" -> LIKE_WEIGHT;
            case "COMMENT", "COMMENT_POST" -> COMMENT_WEIGHT;
            default -> 0d;
        };
    }

    private int safeSize(List<Double> vector) {
        return vector == null ? 0 : vector.size();
    }

    private double valueAt(List<Double> vector, int index) {
        if (vector == null || index < 0 || index >= vector.size() || vector.get(index) == null) {
            return 0d;
        }
        return vector.get(index);
    }
}
