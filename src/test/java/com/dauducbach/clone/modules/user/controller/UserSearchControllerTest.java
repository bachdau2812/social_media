package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.dto.response.SearchSuggestionResponse;
import com.dauducbach.clone.modules.user.service.SearchSuggestionService;
import com.dauducbach.clone.modules.user.service.UserSearchService;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserSearchControllerTest {
    @Test
    void getSuggestionsReturnsSuggestionItems() {
        SearchSuggestionService suggestionService = mock(SearchSuggestionService.class);
        UserSearchService userSearchService = mock(UserSearchService.class);
        WebTestClient client = WebTestClient.bindToController(new UserSearchController(suggestionService, userSearchService)).build();

        when(suggestionService.getSuggestions("user-1", "spr", 10)).thenReturn(Mono.just(List.of(
                new SearchSuggestionResponse("spring webflux", "HISTORY", true)
        )));

        client.get()
                .uri("/search/suggestions?userId=user-1&q=spr&limit=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result[0].text").isEqualTo("spring webflux")
                .jsonPath("$.result[0].source").isEqualTo("HISTORY")
                .jsonPath("$.result[0].isHistory").isEqualTo(true)
                .jsonPath("$.result[0].targetUrl").doesNotExist();
    }

    @Test
    void searchRecordsKeywordAndReturnsUserIds() {
        SearchSuggestionService suggestionService = mock(SearchSuggestionService.class);
        UserSearchService userSearchService = mock(UserSearchService.class);
        WebTestClient client = WebTestClient.bindToController(new UserSearchController(suggestionService, userSearchService)).build();

        when(suggestionService.recordSubmittedSearch("user-1", "spring")).thenReturn(Mono.empty());
        when(userSearchService.searchUsers("spring", null, 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of("user-result-1"), 0, 1, 20)));

        client.get()
                .uri("/search?userId=user-1&q=spring")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0]").isEqualTo("user-result-1");
    }
}
