package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import com.dauducbach.clone.modules.user.service.UserVectorQueryService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class FeedVectorServiceTest {
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    PostFeedQueryService postFeedQueryService;
    @Mock
    UserVectorQueryService userVectorQueryService;

    @Test
    void calculateShortTermVectorAppliesDecayWeightAndNormalizes() {
        FeedVectorService service = newService();

        List<Double> result = service.calculateShortTermVector(
                List.of(1.0, 0.0),
                List.of(0.0, 1.0),
                0.7
        );

        assertThat(result).hasSize(2);
        assertThat(vectorLength(result)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.000001));
        assertThat(result.get(0)).isCloseTo(result.get(1), org.assertj.core.data.Offset.offset(0.000001));
    }

    @Test
    void combineLongAndShortTermFallsBackToAvailableVector() {
        FeedVectorService service = newService();

        List<Double> result = service.combineLongAndShortTerm(List.of(), List.of(3.0, 4.0));

        assertThat(result).containsExactly(0.6, 0.8);
    }

    @Test
    void combineWithUserVectorFallbackUsesUserVectorWhenShortTermIsMissing() {
        FeedVectorService service = newService();

        List<Double> result = service.combineWithUserVectorFallback(
                List.of(1.0, 0.0),
                List.of(),
                List.of(0.0, 1.0)
        );

        assertThat(result).hasSize(2);
        assertThat(vectorLength(result)).isCloseTo(1.0, org.assertj.core.data.Offset.offset(0.000001));
        assertThat(result.get(0)).isGreaterThan(result.get(1));
        assertThat(result.get(1)).isGreaterThan(0.0);
    }

    @Test
    void combineWithUserVectorFallbackUsesUserVectorWhenBothPersonalVectorsAreMissing() {
        FeedVectorService service = newService();

        List<Double> result = service.combineWithUserVectorFallback(List.of(), List.of(), List.of(3.0, 4.0));

        assertThat(result.get(0)).isCloseTo(0.6, org.assertj.core.data.Offset.offset(0.000001));
        assertThat(result.get(1)).isCloseTo(0.8, org.assertj.core.data.Offset.offset(0.000001));
    }

    private FeedVectorService newService() {
        return new FeedVectorService(redisTemplate, postFeedQueryService, userVectorQueryService);
    }

    private double vectorLength(List<Double> vector) {
        return Math.sqrt(vector.stream().mapToDouble(value -> value * value).sum());
    }
}
