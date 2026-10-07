package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.SearchKeyword;
import com.dauducbach.clone.modules.user.entity.UserSearchHistory;
import com.dauducbach.clone.modules.user.repository.SearchKeywordRepository;
import com.dauducbach.clone.modules.user.repository.UserSearchHistoryRepository;
import com.dauducbach.clone.modules.user.search.application.SearchSuggestionCache;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.time.Duration;
import java.util.List;
import java.time.LocalDate;

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
    SearchSuggestionCache suggestionCache;

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

        when(suggestionCache.historyKeywords("user-1", 200)).thenReturn(Mono.just(List.of()));
        when(suggestionCache.putHistoryKeywords(eq("user-1"), any(), eq(Duration.ofHours(3)))).thenReturn(Mono.empty());
        when(userSearchHistoryRepository.findRecentActiveByUserId("user-1", 200)).thenReturn(Flux.just(history));
        when(suggestionCache.globalPrefixSuggestions("spr", 19)).thenReturn(Mono.just(List.of()));
        when(searchKeywordRepository.findPublicByPrefix("spr%", 3L, 19)).thenReturn(Flux.just(SearchKeyword.builder()
                .id(1L)
                .keyword("Spring Boot")
                .normalizedKeyword("spring boot")
                .searchCount(9L)
                .userCount(4L)
                .build()));
        when(suggestionCache.putGlobalPrefixSuggestions(eq("spr"), eq(19), any(), eq(Duration.ofMinutes(5))))
                .thenReturn(Mono.empty());

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
        when(suggestionCache.incrementTrending(any(LocalDate.class), eq("spring webflux"), eq(Duration.ofDays(14))))
                .thenReturn(Mono.empty());
        when(suggestionCache.addHistoryKeyword(eq("user-1"), eq("spring webflux"), anyDouble(), eq(Duration.ofHours(3))))
                .thenReturn(Mono.empty());

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
        when(suggestionCache.incrementTrending(any(LocalDate.class), eq("spring webflux"), eq(Duration.ofDays(14))))
                .thenReturn(Mono.empty());
        when(suggestionCache.addHistoryKeyword(eq("user-1"), eq("spring webflux"), anyDouble(), eq(Duration.ofHours(3))))
                .thenReturn(Mono.empty());

        StepVerifier.create(service.recordSubmittedSearch("user-1", "Spring WebFlux"))
                .verifyComplete();

        verify(userSearchHistoryRepository).incrementHistoryById("history-1", "Spring WebFlux");
        verify(userSearchHistoryRepository, never()).insertHistory(anyString(), anyString(), anyString(), anyString());
        verify(searchKeywordRepository).upsertKeyword("Spring WebFlux", "spring webflux", 0);
    }

    private SearchSuggestionService newService() {
        return new SearchSuggestionService(userSearchHistoryRepository, searchKeywordRepository, suggestionCache);
    }
}
