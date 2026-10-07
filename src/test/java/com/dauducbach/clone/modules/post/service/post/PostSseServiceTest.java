package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostSseServiceTest {

    @Test
    void legacyPostFacadeDelegatesToTheSharedPublisher() {
        UserSsePublisher publisher = mock(UserSsePublisher.class);
        when(publisher.sendToUser("user-1", "post_upload", "payload")).thenReturn(Mono.empty());
        PostSseService facade = new PostSseService(publisher);

        StepVerifier.create(facade.sendToUser("user-1", "post_upload", "payload"))
                .verifyComplete();

        verify(publisher).sendToUser("user-1", "post_upload", "payload");
    }

    @Test
    void transportPublisherIsOwnedByInfrastructureRatherThanPost() {
        assertThat(UserSsePublisher.class.isAssignableFrom(PostSseService.class)).isFalse();
    }
}
