package com.dauducbach.clone.modules.personalization.snapshots;

import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.commons.vector.VectorMath;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class UserVectorQueryService {
    private static final Logger log = LoggerFactory.getLogger(UserVectorQueryService.class);
    private static final AtomicLong lastMissingLogNanos = new AtomicLong();
    private static final long MISSING_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    ReactiveElasticsearchOperations elasticsearchOperations;

    /** Raw metadata is retained so repair can distinguish legacy/unknown embedding spaces. */
    public Mono<UserDetailVector> getSnapshot(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.empty();
        }
        return elasticsearchOperations.get(userId, UserDetailVector.class);
    }

    public Mono<List<Double>> getLongTermVector(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(List.of());
        }

        return getSnapshot(userId)
                .flatMap(document -> Mono.justOrEmpty(document.getUserLongTermVector()))
                .filter(vector -> !vector.isEmpty())
                .switchIfEmpty(missingVector(userId, "long-term"));
    }

    public Mono<List<Double>> getUserVector(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(List.of());
        }

        return getSnapshot(userId)
                .flatMap(document -> Mono.justOrEmpty(document.getUserVector()))
                .filter(vector -> !vector.isEmpty())
                .switchIfEmpty(missingVector(userId, "profile"));
    }

    private Mono<List<Double>> missingVector(String userId, String field) {
        return Mono.fromSupplier(() -> {
            long now = System.nanoTime();
            long previous = lastMissingLogNanos.get();
            if ((previous == 0 || now - previous >= MISSING_LOG_INTERVAL_NANOS)
                    && lastMissingLogNanos.compareAndSet(previous, now)) {
                log.debug("|UserVectorQueryService|missing-vector|userId={}|field={}", userId, field);
            }
            return List.of();
        });
    }

    public Mono<List<Double>> getLongTermOrUserVector(String userId) {
        return getLongTermVector(userId)
                .flatMap(vector -> vector.isEmpty() ? getUserVector(userId) : Mono.just(vector));
    }

    /** Validate the captured blend base independently; unknown nonempty LT needs repair, never a reset. */
    public List<Double> compatibleLongTermBase(UserDetailVector document) {
        if (Boolean.TRUE.equals(document.getDeleted()))
            throw new IllegalStateException("User vector tombstone; repair required for existing SQL user");
        List<Double> longTerm = document.getUserLongTermVector();
        if (longTerm != null && !longTerm.isEmpty()) {
            try {
                VectorMath.requireCompatible(document.getUserLongTermVectorModel(), document.getUserLongTermVectorDimension(),
                        document.getUserLongTermVectorSchemaVersion());
                return VectorMath.normalize(longTerm);
            } catch (IllegalArgumentException error) {
                throw new IllegalStateException("Long-term vector repair required; preserve existing learned history", error);
            }
        }
        List<Double> profile = document.getUserVector();
        if (profile == null || profile.isEmpty()) return List.of();
        VectorMath.requireCompatible(document.getUserVectorModel(), document.getUserVectorDimension(),
                document.getUserVectorSchemaVersion());
        return VectorMath.normalize(profile);
    }
}
