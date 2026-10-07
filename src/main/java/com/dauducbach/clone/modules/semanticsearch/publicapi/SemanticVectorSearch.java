package com.dauducbach.clone.modules.semanticsearch.publicapi;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

/** Read-only semantic search across the application's user and post vector documents. */
public interface SemanticVectorSearch {
    Mono<List<String>> searchUserIds(String query, int limit, Set<String> excludedIds);

    Mono<List<String>> searchUserIdsByVector(List<Double> vector, int limit, Set<String> excludedIds);

    Mono<List<String>> searchPostIds(String query, int limit, Set<String> excludedIds);
}
