package com.dauducbach.clone.modules.personalization.discovery;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

public interface SimilarUserSearch {
    Mono<List<String>> findSimilarUserIds(List<Double> vector, int limit, Set<String> excludedIds);
}
