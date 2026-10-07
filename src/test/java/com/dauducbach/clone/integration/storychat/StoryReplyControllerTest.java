package com.dauducbach.clone.integration.storychat;

import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StoryReplyControllerTest {
    @Test
    void keepsTheExistingReplyRouteAndResponseShape() {
        StoryReplyUseCase useCase = mock(StoryReplyUseCase.class);
        String clientMessageId = UUID.randomUUID().toString();
        StoryReplyRequest request = new StoryReplyRequest("hello", clientMessageId, 1500L);
        when(useCase.reply("story-1", "sender-1", request))
                .thenReturn(Mono.just(new StoryReplyResponse("conversation-1", "message-1", 12)));
        WebTestClient client = WebTestClient.bindToController(new StoryReplyController(useCase))
                .webFilter((exchange, chain) -> chain.filter(exchange.mutate()
                        .principal(Mono.just(new TestingAuthenticationToken("sender-1", "n/a")))
                        .build()))
                .build();

        client.post()
                .uri("/profile-media/stories/story-1/replies")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.message").isEqualTo("Story reply sent")
                .jsonPath("$.result.conversationId").isEqualTo("conversation-1")
                .jsonPath("$.result.messageId").isEqualTo("message-1")
                .jsonPath("$.result.messageSeq").isEqualTo(12);
    }
}
