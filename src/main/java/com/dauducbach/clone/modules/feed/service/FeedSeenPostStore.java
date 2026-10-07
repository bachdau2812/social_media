package com.dauducbach.clone.modules.feed.service;

import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

/** Feed's persistence boundary for best-effort viewer history. */
public interface FeedSeenPostStore {
    Mono<Set<String>> load(String viewerId);

    Mono<Void> mark(String viewerId, List<String> postIds);

    Mono<Void> clear(String viewerId);
}
