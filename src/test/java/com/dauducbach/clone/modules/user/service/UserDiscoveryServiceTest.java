package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.infrastructure.SemanticVectorSearchService;
import com.dauducbach.clone.modules.user.dto.response.UserDiscoveryResponse;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserDiscoveryServiceTest {
    @Mock
    UserSearchService userSearchService;
    @Mock
    UserDiscoveryHydrator hydrator;
    @Mock
    UserVectorQueryService userVectorQueryService;
    @Mock
    SemanticVectorSearchService semanticVectorSearchService;
    @Mock
    UserDetailsRepository userDetailsRepository;
    @Mock
    ReactiveRedisTemplate<String, String> redisTemplate;

    @Test
    void richSearchPreservesSearchPageAndHydratesEveryUser() {
        UserDiscoveryService service = new UserDiscoveryService(
                userSearchService,
                hydrator,
                userVectorQueryService,
                semanticVectorSearchService,
                userDetailsRepository,
                redisTemplate
        );
        UserDiscoveryResponse first = user("user-1");
        UserDiscoveryResponse second = user("user-2");

        when(userSearchService.searchUsers("bach", null, 1, 2))
                .thenReturn(Mono.just(PageResponse.of(List.of("user-1", "user-2"), 1, 7, 2)));
        when(hydrator.hydrate("viewer-1", "user-1")).thenReturn(Mono.just(first));
        when(hydrator.hydrate("viewer-1", "user-2")).thenReturn(Mono.just(second));

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
        UserDiscoveryService service = new UserDiscoveryService(
                userSearchService,
                hydrator,
                userVectorQueryService,
                semanticVectorSearchService,
                userDetailsRepository,
                redisTemplate
        );
        List<Double> targetVector = List.of(0.1, 0.2, 0.3);
        UserDiscoveryResponse result = user("user-4");

        when(userVectorQueryService.getLongTermOrUserVector("target-1")).thenReturn(Mono.just(targetVector));
        when(semanticVectorSearchService.searchUserIdsByVector(
                targetVector,
                4,
                Set.of("target-1", "viewer-1")
        )).thenReturn(Mono.just(List.of("user-2", "user-3", "user-4", "user-5")));
        when(hydrator.hydrate("viewer-1", "user-4")).thenReturn(Mono.just(result));
        when(hydrator.hydrate("viewer-1", "user-5")).thenReturn(Mono.empty());

        StepVerifier.create(service.findSimilar("viewer-1", "target-1", 1, 2))
                .assertNext(page -> {
                    assertThat(page.content()).containsExactly(result);
                    assertThat(page.pageNumber()).isEqualTo(1);
                    assertThat(page.totalElements()).isEqualTo(4);
                    assertThat(page.totalPages()).isEqualTo(2);
                })
                .verifyComplete();

        verify(semanticVectorSearchService).searchUserIdsByVector(
                targetVector,
                4,
                Set.of("target-1", "viewer-1")
        );
    }

    @Test
    void similarUsersReturnsEmptyPageWhenTargetHasNoVector() {
        UserDiscoveryService service = new UserDiscoveryService(
                userSearchService,
                hydrator,
                userVectorQueryService,
                semanticVectorSearchService,
                userDetailsRepository,
                redisTemplate
        );
        when(userVectorQueryService.getLongTermOrUserVector("target-1")).thenReturn(Mono.just(List.of()));

        StepVerifier.create(service.findSimilar("viewer-1", "target-1", 0, 20))
                .assertNext(page -> {
                    assertThat(page.content()).isEmpty();
                    assertThat(page.totalElements()).isZero();
                })
                .verifyComplete();
    }

    @Test
    void similarUsersBoundsVeryLargePageRequestsWithoutIntegerOverflow() {
        UserDiscoveryService service = new UserDiscoveryService(
                userSearchService,
                hydrator,
                userVectorQueryService,
                semanticVectorSearchService,
                userDetailsRepository,
                redisTemplate
        );
        List<Double> targetVector = List.of(0.1, 0.2);

        when(userVectorQueryService.getLongTermOrUserVector("target-1")).thenReturn(Mono.just(targetVector));
        when(semanticVectorSearchService.searchUserIdsByVector(
                targetVector,
                200,
                Set.of("target-1", "viewer-1")
        )).thenReturn(Mono.just(List.of()));

        StepVerifier.create(service.findSimilar("viewer-1", "target-1", Integer.MAX_VALUE, 50))
                .assertNext(page -> {
                    assertThat(page.pageNumber()).isEqualTo(Integer.MAX_VALUE);
                    assertThat(page.content()).isEmpty();
                })
                .verifyComplete();
    }

    private UserDiscoveryResponse user(String userId) {
        return new UserDiscoveryResponse(
                userId,
                userId,
                "Full " + userId,
                "https://cdn/avatar.jpg",
                false,
                false,
                false,
                "NONE"
        );
    }
}
