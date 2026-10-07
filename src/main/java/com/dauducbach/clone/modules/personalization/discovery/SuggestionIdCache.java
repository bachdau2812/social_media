package com.dauducbach.clone.modules.personalization.discovery;

import reactor.core.publisher.Mono;

import java.util.List;

public interface SuggestionIdCache {
    Mono<List<String>> get(String viewerId);

    Mono<Void> put(String viewerId, List<String> userIds);
}
