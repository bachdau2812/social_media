package com.dauducbach.clone.modules.user.search.application;

import com.dauducbach.clone.modules.user.dto.response.SearchSuggestionResponse;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDate;
import java.util.List;

/** Optional suggestion cache; failures are treated as empty reads or ignored writes. */
public interface SearchSuggestionCache {
    Mono<List<String>> historyKeywords(String userId, int limit);

    Mono<Void> putHistoryKeywords(String userId, List<HistoryKeyword> keywords, Duration ttl);

    Mono<Void> addHistoryKeyword(String userId, String normalizedKeyword, double score, Duration ttl);

    Mono<Void> removeHistoryKeyword(String userId, String normalizedKeyword);

    Mono<Void> clearHistory(String userId);

    Mono<List<SearchSuggestionResponse>> globalPrefixSuggestions(String prefix, int limit);

    Mono<Void> putGlobalPrefixSuggestions(
            String prefix,
            int limit,
            List<SearchSuggestionResponse> suggestions,
            Duration ttl);

    Mono<List<String>> trendingKeywords(LocalDate day, int limit);

    Mono<Void> incrementTrending(LocalDate day, String normalizedKeyword, Duration ttl);

    record HistoryKeyword(String normalizedKeyword, double score) { }
}
