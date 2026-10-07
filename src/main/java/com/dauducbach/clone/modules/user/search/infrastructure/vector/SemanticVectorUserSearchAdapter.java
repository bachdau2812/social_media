package com.dauducbach.clone.modules.user.search.infrastructure.vector;

import com.dauducbach.clone.modules.user.search.application.SemanticUserSearch;
import com.dauducbach.clone.modules.semanticsearch.publicapi.SemanticVectorSearch;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

@Component
@RequiredArgsConstructor
public class SemanticVectorUserSearchAdapter implements SemanticUserSearch {
    private final SemanticVectorSearch semanticVectorSearchService;

    @Override
    public Mono<List<String>> searchUserIds(String query, int limit, Set<String> excludedIds) {
        return semanticVectorSearchService.searchUserIds(query, limit, excludedIds);
    }
}
