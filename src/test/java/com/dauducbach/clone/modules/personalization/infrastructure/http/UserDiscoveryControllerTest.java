package com.dauducbach.clone.modules.personalization.infrastructure.http;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.publicapi.UserDiscoveryResponse;
import com.dauducbach.clone.modules.personalization.discovery.UserDiscoveryUseCase;
import com.dauducbach.clone.modules.personalization.infrastructure.http.UserDiscoveryController;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserDiscoveryControllerTest {
    @Test
    void richUserSearchIsExposedWithoutChangingLegacySearchRoute() {
        UserDiscoveryUseCase service = mock(UserDiscoveryUseCase.class);
        WebTestClient client = WebTestClient.bindToController(new UserDiscoveryController(service)).build();
        UserDiscoveryResponse result = user("user-1");

        when(service.search("viewer-1", "bach", null, 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of(result), 0, 1, 20)));

        client.get()
                .uri("/search/users/rich?viewerId=viewer-1&q=bach")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0].userId").isEqualTo("user-1")
                .jsonPath("$.result.content[0].relationship").isEqualTo("NONE");
    }

    @Test
    void similarUsersEndpointUsesTargetProfileAndViewer() {
        UserDiscoveryUseCase service = mock(UserDiscoveryUseCase.class);
        WebTestClient client = WebTestClient.bindToController(new UserDiscoveryController(service)).build();
        UserDiscoveryResponse result = user("user-2");

        when(service.findSimilar("viewer-1", "target-1", 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of(result), 0, 1, 20)));

        client.get()
                .uri("/search/users/target-1/similar?viewerId=viewer-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0].userId").isEqualTo("user-2");
    }

    private UserDiscoveryResponse user(String id) {
        return new UserDiscoveryResponse(id, id, id, "", false, false, false, "NONE");
    }
}
