package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.dto.event.FeedInteractionEvent;
import com.dauducbach.clone.infrastructure.vector.*;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.utils.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class FeedVectorService {
    private static final Logger log = LoggerFactory.getLogger(FeedVectorService.class);
    private static final double SHORT_TERM_DECAY = 0.7d;
    private static final Type DOUBLE_LIST_TYPE = new TypeToken<List<Double>>() {
    }.getType();

    PostFeedQueryService postFeedQueryService;
    FeedVectorSnapshotService snapshots;
    com.dauducbach.clone.modules.user.service.UserVectorSnapshotService userSnapshots;
    FeedInteractionWeightPolicy weightPolicy;
    VectorRedisState vectorRedis;
    UserVectorCoordinator coordinator;

    /** Prepare only. Durable processing owns persistence and the fenced atomic Redis commit. */
    public Mono<List<Double>> updateShortTermVector(FeedInteractionEvent event, VectorLease lease) {
        return Mono.deferContextual(context -> {
            if (!context.hasKey(RecommendationInput.class))
                return Mono.error(new IllegalArgumentException("Fetch recommendation before acquiring lease and supply RecommendationInput"));
            RecommendationInput input = context.get(RecommendationInput.class);
            if (!event.postId().equals(input.postId())) return Mono.error(new IllegalArgumentException("Wrong prepared recommendation"));
            return updateShortTermVector(event, lease, input.vector());
        });
    }

    public record RecommendationInput(String postId, List<Double> vector) {
        public RecommendationInput { vector = List.copyOf(vector); }
    }

    public Mono<List<Double>> recommendation(FeedInteractionEvent event) {
        return postFeedQueryService.getPostRecommendationVector(event.postId())
                .switchIfEmpty(Mono.error(new IllegalStateException("Recommendation getter returned no readiness outcome")));
    }

    /** Consumer fetches recommendation before acquiring the lease and supplies that immutable input. */
    public Mono<List<Double>> updateShortTermVector(FeedInteractionEvent event, VectorLease lease, List<Double> post) {
        if (!event.userId().equals(lease.userId())) return Mono.error(new IllegalArgumentException("Wrong user lease"));
        return coordinator.requireOwner(lease).then(loadShortTermForUpdate(event.userId()))
                .map(old -> post.isEmpty() ? List.<Double>of() : calculateShortTermVector(old, post, weightPolicy.shortWeight(event.action())));
    }

    /** Infrastructure errors propagate. Legacy bytes without known provenance are not mixed. */
    public Mono<List<Double>> loadShortTermForUpdate(String userId) {
        return vectorRedis.shortTermModel(userId).filter(VectorMath.MODEL::equals)
                .flatMap(model -> vectorRedis.shortTerm(userId)).map(json -> {
                    List<Double> parsed = GsonUtils.getGson().fromJson(json, DOUBLE_LIST_TYPE);
                    return VectorMath.normalize(parsed);
                }).defaultIfEmpty(List.of());
    }

    public Mono<List<Double>> buildQueryVector(String userId) {
        return snapshots.load(userId).map(snapshot -> snapshot.queryVector()).defaultIfEmpty(List.of());
    }

    public Mono<List<Double>> getShortTermVector(String userId) {
        return userSnapshots.load(userId).map(snapshot -> snapshot.shortTerm()).defaultIfEmpty(List.of());
    }

    List<Double> calculateShortTermVector(List<Double> oldVector, List<Double> postVector, double weight) {
        if (postVector == null || postVector.isEmpty()) {
            return List.of();
        }

        List<Double> post = VectorMath.normalize(postVector);
        if (oldVector == null || oldVector.isEmpty()) return post;
        List<Double> old = VectorMath.normalize(oldVector);
        List<Double> combined = new ArrayList<>(VectorMath.DIMENSION);
        boolean nonzero = false;
        for (int i = 0; i < VectorMath.DIMENSION; i++) {
            double value = SHORT_TERM_DECAY * old.get(i) + weight * post.get(i);
            combined.add(value);
            nonzero |= value != 0d;
        }
        if (!nonzero) {
            log.warn("ST directions cancelled exactly; using latest post direction");
            return post;
        }
        return VectorMath.normalize(combined);
    }

}
