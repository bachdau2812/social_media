package com.dauducbach.clone.modules.user.search.application;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

public interface SemanticUserSearch {
    Mono<List<String>> searchUserIds(String query, int limit, Set<String> excludedIds);
}
