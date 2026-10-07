package com.dauducbach.clone.modules.personalization.discovery;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceQuery;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryProfile;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryQuery;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.publicapi.UserSearchQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserDiscoveryUseCaseTest {
    @Mock UserSearchQuery userSearch;
    @Mock UserDiscoveryQuery userProfiles;
    @Mock PreferenceQuery preferences;
    @Mock SimilarUserSearch similarUsers;
    @Mock SuggestionIdCache suggestionCache;

    @Test
    void scheduledSuggestionRefreshIsReturnedAsColdWork() {
        when(userProfiles.findAllUserIds()).thenReturn(Flux.empty());
        Mono<Void> scheduledWork = service().refreshDailySuggestions();

        verify(userProfiles, never()).findAllUserIds();
        StepVerifier.create(scheduledWork).verifyComplete();
        verify(userProfiles).findAllUserIds();
    }

    @Test
    void richSearchPreservesSearchPageAndHydratesEveryUser() {
        UserDiscoveryUseCase service = service();
        UserDiscoveryResponse first = user("user-1");
        UserDiscoveryResponse second = user("user-2");
        when(userSearch.searchUsers("bach", null, 1, 2))
                .thenReturn(Mono.just(PageResponse.of(List.of("user-1", "user-2"), 1, 7, 2)));
        when(userProfiles.hydrate("viewer-1", "user-1")).thenReturn(Mono.just(first));
        when(userProfiles.hydrate("viewer-1", "user-2")).thenReturn(Mono.just(second));

        StepVerifier.create(service.search("viewer-1", "bach", null, 1, 2))
                .assertNext(page -> {
                    assertThat(page.content()).containsExactly(first, second);
                    assertThat(page.pageNumber()).isEqualTo(1);
                    assertThat(page.totalElements()).isEqualTo(7);
                    assertThat(page.totalPages()).isEqualTo(4);
                })
                .verifyComplete();
    }

    @Test
    void similarUsersUsesTargetLongTermVectorAndExcludesTargetAndViewer() {
        UserDiscoveryUseCase service = service();
        List<Double> targetVector = List.of(0.1, 0.2, 0.3);
        UserDiscoveryResponse result = user("user-4");
        when(preferences.load("target-1")).thenReturn(Mono.just(preferences(1, List.of(), targetVector)));
        when(similarUsers.findSimilarUserIds(targetVector, 4, Set.of("target-1", "viewer-1")))
                .thenReturn(Mono.just(List.of("user-2", "user-3", "user-4", "user-5")));
        when(userProfiles.hydrate("viewer-1", "user-4")).thenReturn(Mono.just(result));
        when(userProfiles.hydrate("viewer-1", "user-5")).thenReturn(Mono.empty());

        StepVerifier.create(service.findSimilar("viewer-1", "target-1", 1, 2))
                .assertNext(page -> {
                    assertThat(page.content()).containsExactly(result);
                    assertThat(page.pageNumber()).isEqualTo(1);
                    assertThat(page.totalElements()).isEqualTo(4);
                    assertThat(page.totalPages()).isEqualTo(2);
                })
                .verifyComplete();

        verify(similarUsers).findSimilarUserIds(targetVector, 4, Set.of("target-1", "viewer-1"));
    }

    @Test
    void similarUsersReturnsEmptyPageWhenTargetHasNoVector() {
        UserDiscoveryUseCase service = service();
        when(preferences.load("target-1")).thenReturn(Mono.just(preferences(1, List.of(), List.of())));

        StepVerifier.create(service.findSimilar("viewer-1", "target-1", 0, 20))
                .assertNext(page -> {
                    assertThat(page.content()).isEmpty();
                    assertThat(page.totalElements()).isZero();
                })
                .verifyComplete();
    }

    @Test
    void similarUsersBoundsLargePageRequestsWithoutIntegerOverflow() {
        UserDiscoveryUseCase service = service();
        List<Double> targetVector = List.of(0.1, 0.2);
        when(preferences.load("target-1")).thenReturn(Mono.just(preferences(1, List.of(), targetVector)));
        when(similarUsers.findSimilarUserIds(targetVector, 200, Set.of("target-1", "viewer-1")))
                .thenReturn(Mono.just(List.of()));

        StepVerifier.create(service.findSimilar("viewer-1", "target-1", Integer.MAX_VALUE, 50))
                .assertNext(page -> {
                    assertThat(page.pageNumber()).isEqualTo(Integer.MAX_VALUE);
                    assertThat(page.content()).isEmpty();
                })
                .verifyComplete();
    }

    @Test
    void emptySuggestionCacheRefreshesThenRanksAndCachesCandidateIds() {
        UserDiscoveryUseCase service = service();
        List<Double> targetVector = List.of(0.1, 0.2);
        UserDiscoveryResponse first = user("candidate-1");
        UserDiscoveryResponse second = user("candidate-2");
        when(suggestionCache.get("viewer-1")).thenReturn(Mono.just(List.of()));
        when(userProfiles.findProfile("viewer-1"))
                .thenReturn(Mono.just(new UserDiscoveryProfile("viewer-1", "HCMC", "Hue", List.of("music"))));
        when(preferences.load("viewer-1")).thenReturn(Mono.just(preferences(2, List.of(), targetVector)));
        when(similarUsers.findSimilarUserIds(targetVector, 50, Set.of("viewer-1")))
                .thenReturn(Mono.just(List.of("candidate-1", "candidate-2")));
        when(userProfiles.hydrate("viewer-1", "candidate-1")).thenReturn(Mono.just(first));
        when(userProfiles.hydrate("viewer-1", "candidate-2")).thenReturn(Mono.just(second));
        when(userProfiles.findProfile("candidate-1"))
                .thenReturn(Mono.just(new UserDiscoveryProfile("candidate-1", "HCMC", "Hue", List.of("music"))));
        when(userProfiles.findProfile("candidate-2"))
                .thenReturn(Mono.just(new UserDiscoveryProfile("candidate-2", "", "", List.of())));
        when(suggestionCache.put("viewer-1", List.of("candidate-1", "candidate-2"))).thenReturn(Mono.empty());

        StepVerifier.create(service.findSuggested("viewer-1", 0, 20))
                .assertNext(page -> assertThat(page.content()).containsExactly(first, second))
                .verifyComplete();
    }

    private UserDiscoveryUseCase service() {
        return new UserDiscoveryUseCase(userSearch, userProfiles, preferences, similarUsers, suggestionCache);
    }

    private PreferenceSnapshot preferences(long version, List<Double> profile, List<Double> longTerm) {
        return new PreferenceSnapshot(version, profile, longTerm, List.of(), true, "gemini-embedding-2");
    }

    private UserDiscoveryResponse user(String userId) {
        return new UserDiscoveryResponse(userId, userId, "Full " + userId, "https://cdn/avatar.jpg",
                false, false, false, "NONE");
    }
}
