package com.dauducbach.clone.utils;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.assertj.core.api.Assertions.assertThat;

class GetVectorEmbeddingTest {
    static String response(int axis, Integer tokens) {
        List<Double> values = new ArrayList<>(Collections.nCopies(768, 0.0)); values.set(axis, 1.0);
        return "{\"embedding\":{\"values\":" + values + "}" + (tokens == null ? "" : ",\"usageMetadata\":{\"promptTokenCount\":"+tokens+"}") + "}";
    }
    @Test void tokenLimitRejectionSplitsAndUsesActualTokenWeights() {
        AtomicInteger calls = new AtomicInteger();
        GetVectorEmbedding service = new GetVectorEmbedding(WebClient.builder().exchangeFunction(request -> {
            int call = calls.getAndIncrement();
            return Mono.just(ClientResponse.create(call == 0 ? HttpStatus.BAD_REQUEST : HttpStatus.OK)
                    .header("Content-Type", "application/json")
                    .body(call == 0 ? "{\"error\":{\"message\":\"Input token count exceeds maximum of 8192 tokens\"}}" : response(call-1, call == 1 ? 2 : 6)).build());
        }).build());
        StepVerifier.create(service.getEmbedding("first 😀 second"))
                .assertNext(vector -> { assertThat(vector.get(0)).isCloseTo(2 / Math.sqrt(40), org.assertj.core.data.Offset.offset(1e-12));
                    assertThat(vector.get(1)).isCloseTo(6 / Math.sqrt(40), org.assertj.core.data.Offset.offset(1e-12)); }).verifyComplete();
        assertThat(calls).hasValue(3);
    }
    @Test void unrelatedBadRequestIsNotSplit() {
        AtomicInteger calls = new AtomicInteger();
        GetVectorEmbedding service = new GetVectorEmbedding(WebClient.builder().exchangeFunction(request -> {
            calls.incrementAndGet(); return Mono.just(ClientResponse.create(HttpStatus.BAD_REQUEST).body("invalid API key").build());
        }).build());
        StepVerifier.create(service.getEmbedding("some text")).expectError().verify();
        assertThat(calls).hasValue(1);
    }
}
