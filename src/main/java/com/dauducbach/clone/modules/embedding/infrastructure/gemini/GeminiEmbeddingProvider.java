package com.dauducbach.clone.modules.embedding.infrastructure.gemini;

import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.embedding.publicapi.TextEmbeddingProvider;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;

@Component
@RequiredArgsConstructor
public class GeminiEmbeddingProvider implements TextEmbeddingProvider {
    private final WebClient webClient;
    @Value("${gemini-key}") private String apiKey;
    private static final int TOKEN_LIMIT = 8192;

    @Override
    public Mono<List<Double>> getEmbedding(String text) {
        if (text == null || text.isBlank()) return Mono.error(new IllegalArgumentException("Embedding text must not be blank"));
        return chunks(text).collectList().map(chunks -> {
            if (chunks.size() == 1) return chunks.getFirst().vector();
            List<Double> sum = new ArrayList<>(Collections.nCopies(VectorMath.DIMENSION, 0.0));
            for (Chunk chunk : chunks) {
                if (chunk.tokens() == null || chunk.tokens() <= 0)
                    throw new IllegalStateException("Provider usageMetadata.promptTokenCount is required for chunk weighting");
                for (int i = 0; i < sum.size(); i++) sum.set(i, sum.get(i) + chunk.vector().get(i) * chunk.tokens());
            }
            return VectorMath.normalize(sum);
        }).onErrorMap(error -> error instanceof AppException ? error
                : new AppException(ErrorCode.GET_VECTOR_EMBEDDING_FAILED, "Get embedding failed", error));
    }

    private Flux<Chunk> chunks(String text) {
        return embed(text).flatMapMany(chunk -> chunk.tokens() != null && chunk.tokens() > TOKEN_LIMIT
                        ? split(text) : Flux.just(chunk))
                .onErrorResume(WebClientResponseException.class, error -> tokenLimit(error) ? split(text) : Flux.error(error));
    }

    private Flux<Chunk> split(String text) {
        int count = text.codePointCount(0, text.length());
        if (count < 2) return Flux.error(new IllegalStateException("Provider rejected an indivisible embedding input"));
        // Character boundaries select candidates only. The provider enforces token limits; usage supplies weights.
        int boundary = text.offsetByCodePoints(0, count / 2);
        return Flux.concat(Flux.defer(() -> chunks(text.substring(0, boundary))),
                Flux.defer(() -> chunks(text.substring(boundary))));
    }

    private boolean tokenLimit(WebClientResponseException error) {
        if (error.getStatusCode().value() != 400) return false;
        String message = error.getResponseBodyAsString().toLowerCase(Locale.ROOT);
        return message.contains("token") && (message.contains("input") || message.contains("content"))
                && (message.contains("exceed") || message.contains("too many"))
                && (message.contains("maximum") || message.contains("limit") || message.contains("8192"));
    }

    private Mono<Chunk> embed(String text) {
        Map<String, Object> body = Map.of("model", "models/" + VectorMath.MODEL,
                "content", Map.of("parts", List.of(Map.of("text", text))),
                "embedContentConfig", Map.of("outputDimensionality", VectorMath.DIMENSION, "autoTruncate", false));
        return webClient.post().uri("https://generativelanguage.googleapis.com/v1beta/models/" + VectorMath.MODEL + ":embedContent")
                .contentType(MediaType.APPLICATION_JSON).header("x-goog-api-key", apiKey).bodyValue(body)
                .retrieve().bodyToMono(GeminiEmbeddingResponse.class)
                .switchIfEmpty(Mono.error(new IllegalStateException("Empty embedding provider response")))
                .map(response -> new Chunk(VectorMath.normalize(response.embedding() == null ? null : response.embedding().values()),
                        response.usageMetadata() == null ? null : response.usageMetadata().promptTokenCount()));
    }

    private record Chunk(List<Double> vector, Integer tokens) {}
    private record GeminiEmbeddingResponse(Embedding embedding, UsageMetadata usageMetadata) {}
    private record Embedding(List<Double> values) {}
    private record UsageMetadata(Integer promptTokenCount) {}
}
