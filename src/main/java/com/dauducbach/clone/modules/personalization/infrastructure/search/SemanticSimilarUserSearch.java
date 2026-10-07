package com.dauducbach.clone.modules.personalization.infrastructure.search;

import com.dauducbach.clone.modules.personalization.discovery.SimilarUserSearch;
import com.dauducbach.clone.modules.semanticsearch.publicapi.SemanticVectorSearch;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class SemanticSimilarUserSearch implements SimilarUserSearch {
    private final SemanticVectorSearch search;

    @Override
    public Mono<List<String>> findSimilarUserIds(List<Double> vector, int limit, Set<String> excludedIds) {
        return search.searchUserIdsByVector(vector, limit, excludedIds);
    }
}
