package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.search.application.SemanticUserSearch;
import com.dauducbach.clone.modules.user.search.application.UserSearchCriteria;
import com.dauducbach.clone.modules.user.search.application.UserSearchIndex;
import com.dauducbach.clone.modules.user.search.application.UserSearchService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserSearchServiceTest {
    @Mock
    UserSearchIndex userSearchIndex;
    @Mock
    SemanticUserSearch semanticUserSearch;

    @Test
    void searchUsersParsesWhitelistedFiltersAndFillsBySemanticSearch() {
        UserSearchService service = new UserSearchService(userSearchIndex, semanticUserSearch);

        UserSearchCriteria criteria = new UserSearchCriteria("bach%", true, true, true, false, 0, 20);
        when(userSearchIndex.count(criteria)).thenReturn(Mono.just(1L));
        when(userSearchIndex.findIds(criteria))
                .thenReturn(Flux.just("user-db-1"));
        when(semanticUserSearch.searchUserIds(
                "bach",
                19,
                new LinkedHashSet<>(List.of("user-db-1"))
        )).thenReturn(Mono.just(List.of("user-semantic-1", "user-semantic-2")));

        StepVerifier.create(service.searchUsers(" bach ", "hobby+city+unknown", 0, 20))
                .assertNext(response -> {
                    assertThat(response.content()).containsExactly("user-db-1", "user-semantic-1", "user-semantic-2");
                    assertThat(response.totalElements()).isEqualTo(3);
                    assertThat(response.pageNumber()).isZero();
                })
                .verifyComplete();

        verify(userSearchIndex).findIds(criteria);
    }
}
