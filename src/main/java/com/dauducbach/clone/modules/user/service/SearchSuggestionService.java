package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.response.SearchSuggestionResponse;
import com.dauducbach.clone.modules.user.entity.SearchKeyword;
import com.dauducbach.clone.modules.user.entity.UserSearchHistory;
import com.dauducbach.clone.modules.user.repository.SearchKeywordRepository;
import com.dauducbach.clone.modules.user.repository.UserSearchHistoryRepository;
import com.dauducbach.clone.modules.user.search.application.SearchSuggestionCache;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.text.Normalizer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class SearchSuggestionService {
    private static final Logger log = LoggerFactory.getLogger(SearchSuggestionService.class);
    private static final String HISTORY_SOURCE = "HISTORY";
    private static final String GLOBAL_SOURCE = "GLOBAL";
    private static final String TRENDING_SOURCE = "TRENDING";
    private static final Duration HISTORY_CACHE_TTL = Duration.ofHours(3);
    private static final Duration GLOBAL_PREFIX_CACHE_TTL = Duration.ofMinutes(5);
    private static final Duration TRENDING_CACHE_TTL = Duration.ofDays(14);
    private static final int DEFAULT_LIMIT = 10;
    private static final int MAX_LIMIT = 20;
    private static final int HISTORY_CACHE_LOAD_LIMIT = 200;
    private static final int EMPTY_QUERY_HISTORY_LIMIT = 7;
    private static final int MIN_PREFIX_LENGTH = 2;
    private static final int MAX_KEYWORD_LENGTH = 255;
    private static final long GLOBAL_MIN_USER_COUNT = 3L;

    UserSearchHistoryRepository userSearchHistoryRepository;
    SearchKeywordRepository searchKeywordRepository;
    SearchSuggestionCache suggestionCache;

    public Mono<List<SearchSuggestionResponse>> getSuggestions(String userId, String query, int limit) {
        int pageSize = normalizeLimit(limit);
        String normalizedQuery = normalizeForSearch(query);

        Mono<List<SearchSuggestionResponse>> suggestions = normalizedQuery.isBlank()
                ? getDefaultSuggestions(userId, pageSize)
                : getPrefixSuggestions(userId, normalizedQuery, pageSize);

        return suggestions.onErrorResume(error -> {
            log.error("|SearchSuggestionService|getSuggestions|failed|userId={}|queryLength={}|error={}",
                    safeUserId(userId), normalizedQuery.length(), error.getMessage());
            return Mono.just(List.of());
        });
    }

    public Mono<Void> recordSubmittedSearch(String userId, String keyword) {
        String cleanUserId = validateUserId(userId);
        String cleanKeyword = validateKeyword(keyword);
        String normalizedKeyword = normalizeForSearch(cleanKeyword);
        if (normalizedKeyword.isBlank()) {
            return Mono.empty();
        }

        log.info("|SearchSuggestionService|recordSubmittedSearch|userId={}|keywordLength={}|normalizedLength={}",
                cleanUserId, cleanKeyword.length(), normalizedKeyword.length());
        return userSearchHistoryRepository.findByUserIdAndNormalizedKeyword(cleanUserId, normalizedKeyword)
                .flatMap(history -> userSearchHistoryRepository
                        .incrementHistoryById(history.getId(), cleanKeyword)
                        .thenReturn(true))
                .switchIfEmpty(Mono.defer(() -> userSearchHistoryRepository
                        .insertHistory(UUID.randomUUID().toString(), cleanUserId, cleanKeyword, normalizedKeyword)
                        .thenReturn(false)))
                .flatMap(existed -> updateGlobalKeyword(cleanKeyword, normalizedKeyword, existed)
                        .then(updateTrending(normalizedKeyword))
                        .then(updateHistoryCache(cleanUserId, normalizedKeyword))
                        .doOnSuccess(v -> log.info("|SearchSuggestionService|recordSubmittedSearch|success|userId={}|existed={}",
                                cleanUserId, existed)))
                .doOnError(error -> log.error("|SearchSuggestionService|recordSubmittedSearch|failed|userId={}|error={}",
                        cleanUserId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.SEARCH_HISTORY_UPDATE_FAILED,
                                String.format("Update search history failed for userId=%s", cleanUserId),
                                error
                        ));
    }

    public Mono<Void> deleteHistoryKeyword(String userId, String keyword) {
        String cleanUserId = validateUserId(userId);
        String normalizedKeyword = normalizeForSearch(validateKeyword(keyword));

        return userSearchHistoryRepository.deleteKeyword(cleanUserId, normalizedKeyword)
                .then(removeHistoryCacheMember(cleanUserId, normalizedKeyword))
                .doOnSuccess(v -> log.info("|SearchSuggestionService|deleteHistoryKeyword|success|userId={}", cleanUserId))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.SEARCH_HISTORY_UPDATE_FAILED,
                                String.format("Delete search history keyword failed for userId=%s", cleanUserId),
                                error
                        ));
    }

    public Mono<Void> clearHistory(String userId) {
        String cleanUserId = validateUserId(userId);

        return userSearchHistoryRepository.deleteAllByUserId(cleanUserId)
                .then(suggestionCache.clearHistory(cleanUserId))
                .doOnSuccess(v -> log.info("|SearchSuggestionService|clearHistory|success|userId={}", cleanUserId))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.SEARCH_HISTORY_UPDATE_FAILED,
                                String.format("Clear search history failed for userId=%s", cleanUserId),
                                error
                        ));
    }

    private Mono<List<SearchSuggestionResponse>> getDefaultSuggestions(String userId, int limit) {
        int historyLimit = Math.min(EMPTY_QUERY_HISTORY_LIMIT, limit);
        return getHistoryKeywords(userId)
                .map(historyKeywords -> historyKeywords.stream()
                        .limit(historyLimit)
                        .map(keyword -> toSuggestion(keyword, HISTORY_SOURCE, true))
                        .toList())
                .flatMap(historySuggestions -> {
                    int remaining = limit - historySuggestions.size();
                    if (remaining <= 0) {
                        return Mono.just(historySuggestions);
                    }

                    return getTrendingSuggestions(remaining + 10)
                            .flatMap(trendingSuggestions -> {
                                List<SearchSuggestionResponse> mergedWithTrending = mergeSuggestions(limit, historySuggestions, trendingSuggestions);
                                int stillMissing = limit - mergedWithTrending.size();
                                if (stillMissing <= 0) {
                                    return Mono.just(mergedWithTrending);
                                }
                                return getGlobalPopularSuggestions(stillMissing + 10)
                                        .map(globalSuggestions -> mergeSuggestions(limit, mergedWithTrending, globalSuggestions));
                            });
                });
    }

    private Mono<List<SearchSuggestionResponse>> getPrefixSuggestions(String userId, String normalizedQuery, int limit) {
        if (normalizedQuery.length() < MIN_PREFIX_LENGTH) {
            return Mono.just(List.of());
        }

        return getHistoryKeywords(userId)
                .map(historyKeywords -> historyKeywords.stream()
                        .filter(keyword -> keyword.startsWith(normalizedQuery))
                        .limit(limit)
                        .map(keyword -> toSuggestion(keyword, HISTORY_SOURCE, true))
                        .toList())
                .flatMap(historySuggestions -> {
                    int remaining = limit - historySuggestions.size();
                    if (remaining <= 0) {
                        return Mono.just(historySuggestions);
                    }

                    return getGlobalPrefixSuggestions(normalizedQuery, remaining + 10)
                            .map(globalSuggestions -> mergeSuggestions(limit, historySuggestions, globalSuggestions));
                });
    }

    private Mono<List<String>> getHistoryKeywords(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just(List.of());
        }

        String cleanUserId = userId.trim();
        return suggestionCache.historyKeywords(cleanUserId, HISTORY_CACHE_LOAD_LIMIT)
                .flatMap(cachedKeywords -> {
                    if (!cachedKeywords.isEmpty()) {
                        return Mono.just(cachedKeywords);
                    }
                    return loadHistoryFromDatabase(cleanUserId)
                            .flatMap(histories -> cacheHistory(cleanUserId, histories)
                                    .thenReturn(histories.stream()
                                            .map(UserSearchHistory::getNormalizedKeyword)
                                            .filter(keyword -> keyword != null && !keyword.isBlank())
                                            .toList()));
                });
    }

    private Mono<List<UserSearchHistory>> loadHistoryFromDatabase(String userId) {
        return userSearchHistoryRepository.findRecentActiveByUserId(userId, HISTORY_CACHE_LOAD_LIMIT)
                .collectList()
                .onErrorResume(error -> {
                    log.error("|SearchSuggestionService|loadHistoryFromDatabase|failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    private Mono<Void> cacheHistory(String userId, List<UserSearchHistory> histories) {
        if (histories.isEmpty()) {
            return Mono.empty();
        }

        List<SearchSuggestionCache.HistoryKeyword> entries = histories.stream()
                .filter(history -> history.getNormalizedKeyword() != null && !history.getNormalizedKeyword().isBlank())
                .map(history -> new SearchSuggestionCache.HistoryKeyword(
                        history.getNormalizedKeyword(), toEpochMilli(history.getLastSearchedAt())))
                .toList();
        return suggestionCache.putHistoryKeywords(userId, entries, HISTORY_CACHE_TTL);
    }

    private Mono<List<SearchSuggestionResponse>> getGlobalPrefixSuggestions(String prefix, int limit) {
        return suggestionCache.globalPrefixSuggestions(prefix, normalizeLimit(limit))
                .flatMap(cachedSuggestions -> !cachedSuggestions.isEmpty()
                        ? Mono.just(cachedSuggestions)
                        : loadGlobalPrefixFromDatabase(prefix, limit)
                        .flatMap(suggestions -> cacheGlobalPrefix(prefix, limit, suggestions).thenReturn(suggestions)));
    }

    private Mono<List<SearchSuggestionResponse>> loadGlobalPrefixFromDatabase(String prefix, int limit) {
        return searchKeywordRepository.findPublicByPrefix(prefix + "%", GLOBAL_MIN_USER_COUNT, limit)
                .map(keyword -> toSuggestion(keyword.getNormalizedKeyword(), GLOBAL_SOURCE, false))
                .collectList()
                .onErrorResume(error -> {
                    log.error("|SearchSuggestionService|loadGlobalPrefixFromDatabase|failed|prefixLength={}|error={}",
                            prefix.length(), error.getMessage());
                    return Mono.just(List.of());
                });
    }

    private Mono<Void> cacheGlobalPrefix(String prefix, int limit, List<SearchSuggestionResponse> suggestions) {
        return suggestionCache.putGlobalPrefixSuggestions(prefix, normalizeLimit(limit), suggestions, GLOBAL_PREFIX_CACHE_TTL);
    }

    private Mono<List<SearchSuggestionResponse>> getTrendingSuggestions(int limit) {
        return suggestionCache.trendingKeywords(LocalDate.now(ZoneOffset.UTC), normalizeLimit(limit))
                .map(keywords -> keywords.stream()
                        .map(keyword -> toSuggestion(keyword, TRENDING_SOURCE, false))
                        .toList());
    }

    private Mono<List<SearchSuggestionResponse>> getGlobalPopularSuggestions(int limit) {
        return searchKeywordRepository.findPopular(GLOBAL_MIN_USER_COUNT, limit)
                .map(keyword -> toSuggestion(keyword.getNormalizedKeyword(), GLOBAL_SOURCE, false))
                .collectList()
                .onErrorResume(error -> {
                    log.error("|SearchSuggestionService|getGlobalPopularSuggestions|failed|error={}", error.getMessage());
                    return Mono.just(List.of());
                });
    }

    private Mono<Void> updateGlobalKeyword(String keyword, String normalizedKeyword, boolean existedForUser) {
        int userCountDelta = existedForUser ? 0 : 1;
        return searchKeywordRepository.upsertKeyword(keyword, normalizedKeyword, userCountDelta)
                .doOnSuccess(rows -> log.info("|SearchSuggestionService|updateGlobalKeyword|success|normalizedLength={}|userCountDelta={}|rows={}",
                        normalizedKeyword.length(), userCountDelta, rows))
                .doOnError(error -> log.error("|SearchSuggestionService|updateGlobalKeyword|failed|normalizedLength={}|error={}",
                        normalizedKeyword.length(), error.getMessage()))
                .then();
    }

    private Mono<Void> updateTrending(String normalizedKeyword) {
        return suggestionCache.incrementTrending(LocalDate.now(ZoneOffset.UTC), normalizedKeyword, TRENDING_CACHE_TTL);
    }

    private Mono<Void> updateHistoryCache(String userId, String normalizedKeyword) {
        return suggestionCache.addHistoryKeyword(userId, normalizedKeyword, Instant.now().toEpochMilli(), HISTORY_CACHE_TTL);
    }

    private Mono<Void> removeHistoryCacheMember(String userId, String normalizedKeyword) {
        return suggestionCache.removeHistoryKeyword(userId, normalizedKeyword);
    }

    private List<SearchSuggestionResponse> mergeSuggestions(int limit, List<SearchSuggestionResponse>... suggestionGroups) {
        Map<String, SearchSuggestionResponse> merged = new LinkedHashMap<>();
        for (List<SearchSuggestionResponse> suggestions : suggestionGroups) {
            for (SearchSuggestionResponse suggestion : suggestions) {
                if (suggestion == null || suggestion.text() == null || suggestion.text().isBlank()) {
                    continue;
                }
                merged.putIfAbsent(normalizeForSearch(suggestion.text()), suggestion);
                if (merged.size() >= limit) {
                    return merged.values().stream().toList();
                }
            }
        }
        return merged.values().stream().toList();
    }

    private SearchSuggestionResponse toSuggestion(String keyword, String source, boolean history) {
        String text = keyword == null ? "" : keyword.trim();
        return new SearchSuggestionResponse(text, source, history);
    }

    private String validateUserId(String userId) {
        if (userId == null || userId.isBlank()) {
            throw new AppException(ErrorCode.SEARCH_REQUEST_INVALID, "userId is required");
        }
        return userId.trim();
    }

    private String validateKeyword(String keyword) {
        if (keyword == null || keyword.isBlank()) {
            throw new AppException(ErrorCode.SEARCH_REQUEST_INVALID, "keyword is required");
        }
        String cleanKeyword = keyword.trim().replaceAll("\\s+", " ");
        if (cleanKeyword.length() > MAX_KEYWORD_LENGTH) {
            throw new AppException(ErrorCode.SEARCH_REQUEST_INVALID, "keyword is too long");
        }
        return cleanKeyword;
    }

    private int normalizeLimit(int limit) {
        if (limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }

    private String normalizeForSearch(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT);
        String normalized = Normalizer.normalize(trimmed, Normalizer.Form.NFD);
        return normalized.replaceAll("\\p{M}", "");
    }

    private double toEpochMilli(Instant instant) {
        return instant == null ? Instant.now().toEpochMilli() : instant.toEpochMilli();
    }

    private String safeUserId(String userId) {
        return userId == null || userId.isBlank() ? "anonymous" : userId.trim();
    }
}
