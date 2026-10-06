package com.dauducbach.clone.modules.post.controller;
import com.dauducbach.clone.modules.post.dto.request.PostInteractionRequest;
import com.dauducbach.clone.modules.post.dto.response.PostInteractionAcceptedResponse;
import com.dauducbach.clone.modules.post.service.post.*;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.TestingAuthenticationToken;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
class PostInteractionControllerTest {
    @Test void acceptsAuthenticatedActorAndReturns202() {
        var service = mock(PostService.class);
        var controller = new PostController(service,mock(PostSearchService.class),mock(PostDetailQueryService.class));
        var request = new PostInteractionRequest("post",true,31,
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
        var accepted = new PostInteractionAcceptedResponse(request.eventId(),2,false);
        when(service.interact("authenticated-actor",request)).thenReturn(Mono.just(accepted));
        StepVerifier.create(controller.interact(request,new TestingAuthenticationToken("authenticated-actor","secret")))
                .assertNext(response -> {
                    assertEquals(202,response.getStatusCode().value());
                    assertSame(accepted,response.getBody().getResult());
                }).verifyComplete();
        verify(service).interact("authenticated-actor",request);
    }
}
