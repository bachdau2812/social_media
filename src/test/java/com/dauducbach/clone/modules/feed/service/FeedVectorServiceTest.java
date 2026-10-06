package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.infrastructure.vector.UserVectorCoordinator;
import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.infrastructure.vector.VectorRedisState;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.modules.user.service.UserVectorSnapshotService;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FeedVectorServiceTest {
    private final PostFeedQueryService postFeedQueryService = mock(PostFeedQueryService.class);
    private final FeedVectorSnapshotService snapshots = mock(FeedVectorSnapshotService.class);
    private final UserVectorSnapshotService userSnapshots = mock(UserVectorSnapshotService.class);
    private final FeedInteractionWeightPolicy weightPolicy = new FeedInteractionWeightPolicy(1.0, 0.4);
    private final VectorRedisState vectorRedis = mock(VectorRedisState.class);
    private final UserVectorCoordinator coordinator = mock(UserVectorCoordinator.class);

    @Test
    void calculateShortTermVectorDecaysPreviousSignalAndNormalizesCombinedDirection() {
        FeedVectorService service = newService();
        List<Double> old = basis(0, 1.0);
        List<Double> latest = basis(1, 1.0);

        List<Double> result = service.calculateShortTermVector(old, latest, 0.7);

        assertThat(result).hasSize(VectorMath.DIMENSION);
        assertThat(vectorLength(result)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.000001));
        assertThat(result.get(0)).isCloseTo(result.get(1), org.assertj.core.data.Offset.offset(0.000001));
    }

    @Test
    void calculateShortTermVectorUsesLatestDirectionWhenSignalsCancel() {
        FeedVectorService service = newService();
        List<Double> old = basis(0, 1.0);
        List<Double> latest = basis(0, -1.0);

        List<Double> result = service.calculateShortTermVector(old, latest, 0.7);

        assertThat(result).isEqualTo(latest);
    }

    @Test
    void calculateShortTermVectorStartsFromLatestPostWhenHistoryIsMissing() {
        FeedVectorService service = newService();
        List<Double> latest = basis(2, 2.0);

        List<Double> result = service.calculateShortTermVector(List.of(), latest, 0.7);

        assertThat(result).isEqualTo(basis(2, 1.0));
    }

    private FeedVectorService newService() {
        return new FeedVectorService(postFeedQueryService, snapshots, userSnapshots, weightPolicy, vectorRedis, coordinator);
    }

    private List<Double> basis(int index, double value) {
        List<Double> vector = new ArrayList<>(Collections.nCopies(VectorMath.DIMENSION, 0.0));
        vector.set(index, value);
        return vector;
    }

    private double vectorLength(List<Double> vector) {
        return Math.sqrt(vector.stream().mapToDouble(value -> value * value).sum());
    }
}
