package com.dauducbach.clone.infrastructure.vector;

import org.junit.jupiter.api.Test;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.utils.GetVectorEmbedding;
import com.google.gson.Gson;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

class VectorMathTest {
    @Test
    void sameDimensionDoesNotMakeUnknownOrDifferentModelsCompatible() {
        requireCompatible("gemini-embedding-2", 768, 1);
        assertThatThrownBy(() -> requireCompatible(null, 768, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requireCompatible("gemini-embedding-001", 768, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requireCompatible("gemini-embedding-2", null, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requireCompatible("gemini-embedding-2", 767, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requireCompatible("gemini-embedding-2", 768, null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> requireCompatible("gemini-embedding-2", 768, 2)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void embeddingBoundaryNormalizesGeminiOutputAndRejectsWrongDimensions() {
        StepVerifier.create(embeddingClient(oneHot(0, 8)).getEmbedding("profile"))
                .assertNext(vector -> assertThat(vector.getFirst()).isEqualTo(1.0)).verifyComplete();
        StepVerifier.create(embeddingClient(Collections.nCopies(767, 1.0)).getEmbedding("profile"))
                .expectError(AppException.class).verify();
    }

    private GetVectorEmbedding embeddingClient(List<Double> vector) {
        String body = new Gson().toJson(Map.of("embedding", Map.of("values", vector)));
        WebClient webClient = WebClient.builder().exchangeFunction(request -> Mono.just(ClientResponse.create(HttpStatus.OK)
                .header("Content-Type", "application/json").body(body).build())).build();
        return new GetVectorEmbedding(webClient);
    }

    private static void requireCompatible(String model, Integer dimension, Integer schemaVersion) {
        VectorMath.requireCompatible(model, dimension, schemaVersion);
    }
    @Test
    void normalizationRejectsInvalidDimensionsAndValues() {
        assertThatThrownBy(() -> normalize(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> normalize(Collections.nCopies(767, 1.0))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> normalize(Collections.nCopies(769, 1.0))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> normalize(Collections.nCopies(768, 0.0))).isInstanceOf(IllegalArgumentException.class);
        for (Double invalid : new Double[]{null, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY}) {
            List<Double> vector = oneHot(0, 1.0);
            vector.set(1, invalid);
            assertThatThrownBy(() -> normalize(vector)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void normalizeReturnsNewUnitVectorWithoutMutatingInput() {
        List<Double> input = oneHot(0, 4.0);
        List<Double> result = normalize(input);
        assertThat(result).hasSize(768).isNotSameAs(input);
        assertThat(result.getFirst()).isEqualTo(1.0);
        assertThat(input.getFirst()).isEqualTo(4.0);
        assertThat(norm(result)).isCloseTo(1.0, within(1e-12));
    }

    @Test
    void weightedMixNormalizesEachInputAndThenTheResult() {
        List<Double> a = oneHot(0, 100.0);
        List<Double> b = oneHot(1, 0.01);
        List<Double> result = mix(a, 0.7, b, 0.3);
        assertThat(result).hasSize(768);
        assertThat(result.get(0)).isCloseTo(0.7 / Math.sqrt(0.58), within(1e-12));
        assertThat(result.get(1)).isCloseTo(0.3 / Math.sqrt(0.58), within(1e-12));
        assertThat(norm(result)).isCloseTo(1.0, within(1e-12));
        assertThat(a.get(0)).isEqualTo(100.0);
        assertThat(b.get(1)).isEqualTo(0.01);
    }

    @Test
    void normalizationHandlesOverflowAndUnderflowWithoutLosingDirection() {
        for (double value : new double[]{Double.MAX_VALUE, Double.MIN_VALUE}) {
            List<Double> input = oneHot(0, value);
            input.set(1, value);
            List<Double> result = normalize(input);
            assertThat(result.get(0)).isCloseTo(1.0 / Math.sqrt(2), within(1e-12));
            assertThat(norm(result)).isCloseTo(1.0, within(1e-12));
        }
    }

    @Test
    void mixRejectsInvalidWeightsAndCancelledDirection() {
        List<Double> a = oneHot(0, 1.0);
        List<Double> b = oneHot(0, -1.0);
        for (double weight : new double[]{-1.0, Double.NaN, Double.POSITIVE_INFINITY}) {
            assertThatThrownBy(() -> mix(a, weight, b, 1)).isInstanceOf(IllegalArgumentException.class);
        }
        assertThatThrownBy(() -> mix(a, 0, b, 0)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> mix(a, 1, b, 1)).isInstanceOf(IllegalArgumentException.class);
        assertThat(norm(mix(a, Double.MAX_VALUE, oneHot(1, 1), Double.MAX_VALUE)))
                .isCloseTo(1.0, within(1e-12));
    }

    private static List<Double> oneHot(int index, double value) {
        List<Double> values = new ArrayList<>(Collections.nCopies(768, 0.0));
        values.set(index, value);
        return values;
    }

    private static double norm(List<Double> vector) {
        return Math.sqrt(vector.stream().mapToDouble(value -> value * value).sum());
    }

    private static List<Double> normalize(List<Double> vector) {
        return VectorMath.normalize(vector);
    }

    private static List<Double> mix(List<Double> a, double wa, List<Double> b, double wb) {
        return VectorMath.mix(a, wa, b, wb);
    }
}
