package com.dauducbach.clone.modules.user.search.infrastructure.redis;

import com.dauducbach.clone.modules.user.dto.response.SearchSuggestionResponse;
import com.dauducbach.clone.modules.user.search.application.SearchSuggestionCache;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import com.google.gson.reflect.TypeToken;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Type;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;

@Slf4j
@Repository
@RequiredArgsConstructor
public class RedisSearchSuggestionCache implements SearchSuggestionCache {
    private static final String HISTORY_PREFIX = "search:history:";
    private static final String GLOBAL_PREFIX = "search:suggest:global:";
    private static final String TRENDING_PREFIX = "search:trending:";
    private static final Type SUGGESTION_LIST_TYPE = new TypeToken<List<SearchSuggestionResponse>>() { }.getType();

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Override
    public Mono<List<String>> historyKeywords(String userId, int limit) {
        return redisTemplate.opsForZSet()
                .reverseRange(HISTORY_PREFIX + userId, Range.closed(0L, (long) limit - 1))
                .collectList()
                .onErrorResume(error -> {
                    log.warn("Search history cache read failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    @Override
    public Mono<Void> putHistoryKeywords(String userId, List<HistoryKeyword> keywords, Duration ttl) {
        if (keywords.isEmpty()) return Mono.empty();
        String key = HISTORY_PREFIX + userId;
        return Flux.fromIterable(keywords)
                .filter(item -> item.normalizedKeyword() != null && !item.normalizedKeyword().isBlank())
                .flatMap(item -> redisTemplate.opsForZSet().add(key, item.normalizedKeyword(), item.score()))
                .then(redisTemplate.expire(key, ttl))
                .then()
                .onErrorResume(error -> {
                    log.warn("Search history cache write failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> addHistoryKeyword(String userId, String normalizedKeyword, double score, Duration ttl) {
        String key = HISTORY_PREFIX + userId;
        return redisTemplate.opsForZSet().add(key, normalizedKeyword, score)
                .then(redisTemplate.expire(key, ttl))
                .then()
                .onErrorResume(error -> {
                    log.warn("Search history cache update failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> removeHistoryKeyword(String userId, String normalizedKeyword) {
        return redisTemplate.opsForZSet().remove(HISTORY_PREFIX + userId, normalizedKeyword)
                .then()
                .onErrorResume(error -> {
                    log.warn("Search history cache removal failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<Void> clearHistory(String userId) {
        return redisTemplate.delete(HISTORY_PREFIX + userId)
                .then()
                .onErrorResume(error -> {
                    log.warn("Search history cache clear failed|userId={}|error={}", userId, error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<List<SearchSuggestionResponse>> globalPrefixSuggestions(String prefix, int limit) {
        return redisTemplate.opsForValue().get(globalPrefixKey(prefix, limit))
                .map(this::parseSuggestions)
                .filter(suggestions -> !suggestions.isEmpty())
                .defaultIfEmpty(List.of())
                .onErrorResume(error -> {
                    log.warn("Global suggestion cache read failed|prefixLength={}|error={}", prefix.length(), error.getMessage());
                    return Mono.just(List.of());
                });
    }

    @Override
    public Mono<Void> putGlobalPrefixSuggestions(
            String prefix,
            int limit,
            List<SearchSuggestionResponse> suggestions,
            Duration ttl) {
        return redisTemplate.opsForValue()
                .set(globalPrefixKey(prefix, limit), GsonUtils.getGson().toJson(suggestions), ttl)
                .then()
                .onErrorResume(error -> {
                    log.warn("Global suggestion cache write failed|prefixLength={}|error={}", prefix.length(), error.getMessage());
                    return Mono.empty();
                });
    }

    @Override
    public Mono<List<String>> trendingKeywords(LocalDate day, int limit) {
        return redisTemplate.opsForZSet()
                .reverseRange(TRENDING_PREFIX + day, Range.closed(0L, (long) limit - 1))
                .collectList()
                .onErrorResume(error -> {
                    log.warn("Trending suggestion cache read failed|error={}", error.getMessage());
                    return Mono.just(List.of());
                });
    }

    @Override
    public Mono<Void> incrementTrending(LocalDate day, String normalizedKeyword, Duration ttl) {
        String key = TRENDING_PREFIX + day;
        return redisTemplate.opsForZSet().incrementScore(key, normalizedKeyword, 1.0)
                .then(redisTemplate.expire(key, ttl))
                .then()
                .onErrorResume(error -> {
                    log.warn("Trending suggestion cache update failed|error={}", error.getMessage());
                    return Mono.empty();
                });
    }

    private String globalPrefixKey(String prefix, int limit) {
        return GLOBAL_PREFIX + prefix + ":" + limit;
    }

    private List<SearchSuggestionResponse> parseSuggestions(String json) {
        try {
            List<SearchSuggestionResponse> suggestions = GsonUtils.getGson().fromJson(json, SUGGESTION_LIST_TYPE);
            return suggestions == null ? List.of() : suggestions;
        } catch (RuntimeException error) {
            log.warn("Global suggestion cache entry is invalid|error={}", error.getMessage());
            return List.of();
        }
    }
}
