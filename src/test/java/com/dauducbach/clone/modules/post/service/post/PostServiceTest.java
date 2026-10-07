package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.application.PostDetailsCache;
import com.dauducbach.clone.modules.post.application.PostNotificationMuteStore;
import com.dauducbach.clone.modules.post.application.PostPublicationMessaging;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;


import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostServiceTest {
    @Mock
    PostDetailsRepository postDetailsRepository;
    @Mock
    PostItemRepository postItemRepository;
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    PostDetailsCache postDetailsCache;
    @Mock
    PostNotificationMuteStore postNotificationMuteStore;
    @Mock
    PostPublicationMessaging publicationMessaging;
    @Mock
    PostMediaModerationOrchestrator postMediaModerationOrchestrator;

    @Test
    void mutePostNotificationsDelegatesTheExpiringPreferenceToItsStore() {
        PostService service = newService();

        when(postDetailsRepository.existsById("post-1")).thenReturn(Mono.just(true));
        when(postNotificationMuteStore.mute("post-1", "user-1")).thenReturn(Mono.just(true));

        StepVerifier.create(service.mutePostNotifications("post-1", "user-1"))
                .expectNextMatches(response -> response.postId().equals("post-1")
                        && response.userId().equals("user-1")
                        && response.mutedDays() == 60)
                .verifyComplete();

        verify(postNotificationMuteStore).mute("post-1", "user-1");
    }

    @Test
    void mutePostNotificationsRejectsBlankUserId() {
        PostService service = newService();

        StepVerifier.create(Mono.defer(() -> service.mutePostNotifications("post-1", " ")))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.POST_NOTIFICATION_MUTE_FAILED)
                .verify();
    }

    private PostService newService() {
        return new PostService(
                postDetailsRepository,
                postItemRepository,
                r2dbcEntityTemplate,
                postDetailsCache,
                postNotificationMuteStore,
                publicationMessaging,
                postMediaModerationOrchestrator,
                org.mockito.Mockito.mock(PostVectorService.class)
        );
    }
}
