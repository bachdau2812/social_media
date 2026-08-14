package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class PostSseServiceTest {

    @Test
    void sendToUserEmitsToLocalSubscriber() {
        PostSseService service = new PostSseService();

        StepVerifier.create(service.subscribe("user-1"))
                .then(() -> service.sendToUser("user-1", "post_upload", "payload").block())
                .assertNext(event -> {
                    assertThat(event.event()).isEqualTo("post_upload");
                    assertThat(event.data()).isEqualTo("payload");
                })
                .thenCancel()
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void sendToUserIgnoresBlankUserId() {
        PostSseService service = new PostSseService();

        StepVerifier.create(service.sendToUser(" ", "post_upload", "payload"))
                .verifyComplete();
    }

    @Test
    void postSseImplementsNeutralPublisher() {
        assertThat(UserSsePublisher.class).isAssignableFrom(PostSseService.class);
    }
}
