package com.dauducbach.clone.modules.embedding.publicapi;

import reactor.core.publisher.Mono;

import java.util.List;

/** Creates normalized vectors for text without exposing the provider or HTTP client. */
public interface TextEmbeddingProvider {
    Mono<List<Double>> getEmbedding(String text);
}
