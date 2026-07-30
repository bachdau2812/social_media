package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.response.RichPostSearchResponse;
import com.dauducbach.clone.modules.post.service.RichPostSearchService;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RichPostSearchControllerTest {
    @Test
    void richPostSearchHasAnAdditiveRoute() {
        RichPostSearchService service = mock(RichPostSearchService.class);
        WebTestClient client = WebTestClient.bindToController(new RichPostSearchController(service)).build();
        RichPostSearchResponse result = new RichPostSearchResponse(
                "post-1",
                "user-1",
                "bach",
                "Dau Duc Bach",
                "https://cdn/avatar.jpg",
                "caption",
                List.of("spring"),
                "4:3",
                List.of(),
                0,
                Instant.parse("2026-07-28T00:00:00Z")
        );

        when(service.search("spring", 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of(result), 0, 1, 20)));

        client.get()
                .uri("/posts/search/rich?query=spring")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0].postId").isEqualTo("post-1")
                .jsonPath("$.result.content[0].authorAvatarUrl").isEqualTo("https://cdn/avatar.jpg");
    }
}
