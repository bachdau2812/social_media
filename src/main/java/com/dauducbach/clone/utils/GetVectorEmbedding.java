package com.dauducbach.clone.utils;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class GetVectorEmbedding {

    private static final Logger log =
            LoggerFactory.getLogger(GetVectorEmbedding.class);

    private static final String MODEL = "gemini-embedding-2";

    /*
     * Gemini Embedding 2 hỗ trợ 128–3072 chiều.
     * 768 là lựa chọn cân bằng giữa chất lượng và dung lượng lưu trữ.
     */
    private static final int OUTPUT_DIMENSION = 768;

    private final WebClient webClient;

    @Value("${gemini-key}")
    private String apiKey;

    /**
     * Tạo embedding chung cho một đoạn văn bản đã được format.
     */
    public Mono<List<Double>> getEmbedding(String text) {
        if (text == null || text.isBlank()) {
            return Mono.error(
                    new IllegalArgumentException("Embedding text must not be blank")
            );
        }

        log.info(
                "|GetVectorEmbedding|getEmbedding|start|textLength={}",
                text.length()
        );

        String uri =
                "https://generativelanguage.googleapis.com/v1beta/models/"
                        + MODEL
                        + ":embedContent";

        Map<String, Object> body = Map.of(
                "model", "models/" + MODEL,
                "content", Map.of(
                        "parts", List.of(
                                Map.of("text", text)
                        )
                ),
                "output_dimensionality", OUTPUT_DIMENSION
        );

        return webClient.post()
                .uri(uri)
                .contentType(MediaType.APPLICATION_JSON)
                .header("x-goog-api-key", apiKey)
                .bodyValue(body)
                .retrieve()
                .bodyToMono(GeminiEmbeddingResponse.class)
                .map(response -> {
                    if (response.embedding() == null
                            || response.embedding().values() == null) {
                        throw new AppException(
                                ErrorCode.GET_VECTOR_EMBEDDING_FAILED
                        );
                    }

                    return response.embedding().values();
                })
                .doOnSuccess(vector ->
                        log.info(
                                "|GetVectorEmbedding|getEmbedding|success|dimension={}",
                                vector.size()
                        )
                )
                .onErrorMap(error -> {
                    log.error(
                            "|GetVectorEmbedding|getEmbedding|error"
                                    + "|textLength={}|error={}",
                            text.length(),
                            error.getMessage(),
                            error
                    );

                    if (error instanceof AppException) {
                        return error;
                    }

                    return new AppException(
                            ErrorCode.GET_VECTOR_EMBEDDING_FAILED
                    );
                });
    }

    /**
     * Tạo embedding cho tài liệu được lưu trong vector database.
     */
    public Mono<List<Double>> getDocumentEmbedding(
            String title,
            String text
    ) {
        String documentTitle =
                title == null || title.isBlank()
                        ? "none"
                        : title;

        String preparedText =
                "title: " + documentTitle + " | text: " + text;

        return getEmbedding(preparedText);
    }

    /**
     * Tạo embedding cho câu tìm kiếm của người dùng.
     */
    public Mono<List<Double>> getQueryEmbedding(String query) {
        String preparedQuery =
                "task: search result | query: " + query;

        return getEmbedding(preparedQuery);
    }

    private record GeminiEmbeddingResponse(
            Embedding embedding
    ) {
    }

    private record Embedding(
            List<Double> values
    ) {
    }
}