package com.dauducbach.clone.utils;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor

public class GetVectorEmbedding {
    private static final Logger log = LoggerFactory.getLogger(GetVectorEmbedding.class);
    private final WebClient webClient;


    @Value("${gemini-key}")
    private String apiKey;

    public Mono<List<Double>> getEmbedding(String text) {
        log.info("|GetVectorEmbedding|getEmbedding|start|text={}", text);
        String uri = "https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key=" + apiKey;

        Map<String, Object> body = Map.of(
                "model", "models/gemini-embedding-exp-03-07",
                "content", Map.of(
                        "parts", new Object[]{ Map.of("text", text) }
                )
        );

        return webClient.post()
                .uri(uri)
                .header("Content-Type", "application/json")
                .body(BodyInserters.fromValue(body))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .onErrorMap(error -> {
                    log.error("|GetVectorEmbedding|getEmbedding|error|text={}|error={}", text, error.getMessage());
                    return new AppException(ErrorCode.GET_VECTOR_EMBEDDING_FAILED);
                })
                .map(response -> {
                    Map<String, Object> embeddingMap = (Map<String, Object>) response.get("embedding");

                    return (List<Double>) embeddingMap.get("values");
                });
    }
}
