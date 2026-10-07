package com.dauducbach.clone.modules.personalization.interactions;

import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteraction;
import com.dauducbach.clone.commons.vector.*;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.*;
import com.dauducbach.clone.modules.post.publicapi.PostFeedQuery;
import com.dauducbach.clone.commons.serialization.GsonUtils;
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
public class ShortTermPreferenceVectorService {
    private static final Logger log = LoggerFactory.getLogger(ShortTermPreferenceVectorService.class);
    private static final double SHORT_TERM_DECAY = 0.7d;
    private static final Type DOUBLE_LIST_TYPE = new TypeToken<List<Double>>() {
    }.getType();

    PostFeedQuery postFeedQueryService;
    InteractionWeightPolicy weightPolicy;
    VectorRedisState vectorRedis;
    UserVectorCoordinator coordinator;

    public Mono<List<Double>> recommendation(PreferenceInteraction event) {
        return postFeedQueryService.getRecommendationVector(event.postId())
                .switchIfEmpty(Mono.error(new IllegalStateException("Recommendation getter returned no readiness outcome")));
    }

    /** Consumer fetches recommendation before acquiring the lease and supplies that immutable input. */
    public Mono<List<Double>> updateShortTermVector(PreferenceInteraction event, VectorLease lease, List<Double> post) {
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
