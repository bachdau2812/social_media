package com.dauducbach.clone.modules.notification.controller;

import com.dauducbach.clone.modules.notification.service.NotificationSseService;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.core.Authentication;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class NotificationSseControllerTest {

    @Test
    void subscribesTheAuthenticatedPrincipalWithoutAClientUserId() {
        NotificationSseService service = mock(NotificationSseService.class);
        NotificationSseController controller = new NotificationSseController(service);
        Authentication authentication = mock(Authentication.class);
        when(authentication.getName()).thenReturn("user-1");
        when(service.subscribe("user-1")).thenReturn(Flux.just(
                ServerSentEvent.<String>builder().event("notification_changed").data("notification-1").build()));

        StepVerifier.create(controller.stream(authentication))
                .expectNextCount(1)
                .verifyComplete();

        verify(service).subscribe("user-1");
    }
}
