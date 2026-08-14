package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.SearchKeyword;
import com.dauducbach.clone.modules.user.entity.UserSearchHistory;
import com.dauducbach.clone.modules.user.repositoty.SearchKeywordRepository;
import com.dauducbach.clone.modules.user.repositoty.UserSearchHistoryRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.ReactiveZSetOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SearchSuggestionServiceTest {
    @Mock
    UserSearchHistoryRepository userSearchHistoryRepository;
    @Mock
    SearchKeywordRepository searchKeywordRepository;
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;
    @Mock
    ReactiveZSetOperations<String, String> zSetOperations;
    @Mock
    ReactiveValueOperations<String, String> valueOperations;

    @Test
    void getSuggestionsReturnsEmptyWhenPrefixTooShort() {
        SearchSuggestionService service = newService();

        StepVerifier.create(service.getSuggestions("user-1", "s", 10))
                .expectNextMatches(items -> items.isEmpty())
                .verifyComplete();
    }

    @Test
    void getSuggestionsMergesHistoryBeforeGlobalPrefixSuggestions() {
        SearchSuggestionService service = newService();
        UserSearchHistory history = UserSearchHistory.builder()
                .id("history-1")
                .userId("user-1")
                .keyword("Spring WebFlux")
                .normalizedKeyword("spring webflux")
                .lastSearchedAt(Instant.parse("2026-06-20T00:00:00Z"))
                .build();

        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.reverseRange("search:history:user-1", Range.closed(0L, 199L))).thenReturn(Flux.empty());
        when(userSearchHistoryRepository.findRecentActiveByUserId("user-1", 200)).thenReturn(Flux.just(history));
        when(zSetOperations.add(eq("search:history:user-1"), eq("spring webflux"), anyDouble())).thenReturn(Mono.just(true));
        when(redisTemplate.expire(eq("search:history:user-1"), any(Duration.class))).thenReturn(Mono.just(true));
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("search:suggest:global:spr:19")).thenReturn(Mono.empty());
        when(searchKeywordRepository.findPublicByPrefix("spr%", 3L, 19)).thenReturn(Flux.just(SearchKeyword.builder()
                .id(1L)
                .keyword("Spring Boot")
                .normalizedKeyword("spring boot")
                .searchCount(9L)
                .userCount(4L)
                .build()));
        when(valueOperations.set(eq("search:suggest:global:spr:19"), anyString(), any(Duration.class))).thenReturn(Mono.just(true));

        StepVerifier.create(service.getSuggestions("user-1", "spr", 10))
                .assertNext(items -> {
                    assertThat(items).hasSize(2);
                    assertThat(items.get(0).text()).isEqualTo("spring webflux");
                    assertThat(items.get(0).source()).isEqualTo("HISTORY");
                    assertThat(items.get(0).isHistory()).isTrue();
                    assertThat(items.get(1).text()).isEqualTo("spring boot");
                    assertThat(items.get(1).source()).isEqualTo("GLOBAL");
                })
                .verifyComplete();
    }

    @Test
    void recordSubmittedSearchUpdatesDatabaseBeforeRedisCaches() {
        SearchSuggestionService service = newService();

        when(userSearchHistoryRepository.findByUserIdAndNormalizedKeyword("user-1", "spring webflux"))
                .thenReturn(Mono.empty());
        when(userSearchHistoryRepository.insertHistory(anyString(), eq("user-1"), eq("Spring WebFlux"), eq("spring webflux")))
                .thenReturn(Mono.just(1));
        when(searchKeywordRepository.upsertKeyword("Spring WebFlux", "spring webflux", 1)).thenReturn(Mono.just(1));
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.incrementScore(anyString(), eq("spring webflux"), eq(1.0))).thenReturn(Mono.just(1.0));
        when(zSetOperations.add(eq("search:history:user-1"), eq("spring webflux"), anyDouble())).thenReturn(Mono.just(true));
        when(redisTemplate.expire(anyString(), any(Duration.class))).thenReturn(Mono.just(true));

        StepVerifier.create(service.recordSubmittedSearch("user-1", "Spring WebFlux"))
                .verifyComplete();

        verify(userSearchHistoryRepository).insertHistory(anyString(), eq("user-1"), eq("Spring WebFlux"), eq("spring webflux"));
        verify(userSearchHistoryRepository, never()).incrementHistoryById(anyString(), anyString());
        verify(searchKeywordRepository).upsertKeyword("Spring WebFlux", "spring webflux", 1);
    }

    @Test
    void recordSubmittedSearchIncrementsHistoryAndGlobalKeywordWhenHistoryExists() {
        SearchSuggestionService service = newService();

        when(userSearchHistoryRepository.findByUserIdAndNormalizedKeyword("user-1", "spring webflux"))
                .thenReturn(Mono.just(UserSearchHistory.builder()
                        .id("history-1")
                        .userId("user-1")
                        .keyword("Spring WebFlux")
                        .normalizedKeyword("spring webflux")
                        .searchCount(3L)
                        .build()));
        when(userSearchHistoryRepository.incrementHistoryById("history-1", "Spring WebFlux"))
                .thenReturn(Mono.just(1));
        when(searchKeywordRepository.upsertKeyword("Spring WebFlux", "spring webflux", 0)).thenReturn(Mono.just(1));
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(zSetOperations.incrementScore(anyString(), eq("spring webflux"), eq(1.0))).thenReturn(Mono.just(1.0));
        when(zSetOperations.add(eq("search:history:user-1"), eq("spring webflux"), anyDouble())).thenReturn(Mono.just(true));
        when(redisTemplate.expire(anyString(), any(Duration.class))).thenReturn(Mono.just(true));

        StepVerifier.create(service.recordSubmittedSearch("user-1", "Spring WebFlux"))
                .verifyComplete();

        verify(userSearchHistoryRepository).incrementHistoryById("history-1", "Spring WebFlux");
        verify(userSearchHistoryRepository, never()).insertHistory(anyString(), anyString(), anyString(), anyString());
        verify(searchKeywordRepository).upsertKeyword("Spring WebFlux", "spring webflux", 0);
    }

    private SearchSuggestionService newService() {
        return new SearchSuggestionService(userSearchHistoryRepository, searchKeywordRepository, redisTemplate);
    }
}
