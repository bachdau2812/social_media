package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.constant.PostNotificationCacheKeys;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.PostItemRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.test.StepVerifier;

import java.time.Duration;

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
    ReactiveRedisTemplate<String, String> reactiveRedisStringTemplate;
    @Mock
    ReactiveValueOperations<String, String> valueOperations;
    @Mock
    KafkaSender<String, String> kafkaSender;
    @Mock
    PostSseService postSseService;
    @Mock
    PostMediaModerationOrchestrator postMediaModerationOrchestrator;

    @Test
    void mutePostNotificationsStoresRedisKeyForSixtyDays() {
        PostService service = newService();
        String cacheKey = PostNotificationCacheKeys.mutedPostNotification("post-1", "user-1");

        when(postDetailsRepository.existsById("post-1")).thenReturn(Mono.just(true));
        when(reactiveRedisStringTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.set(cacheKey, "true", Duration.ofDays(60))).thenReturn(Mono.just(true));

        StepVerifier.create(service.mutePostNotifications("post-1", "user-1"))
                .expectNextMatches(response -> response.postId().equals("post-1")
                        && response.userId().equals("user-1")
                        && response.mutedDays() == 60)
                .verifyComplete();

        verify(valueOperations).set(cacheKey, "true", Duration.ofDays(60));
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
                reactiveRedisStringTemplate,
                kafkaSender,
                postSseService,
                postMediaModerationOrchestrator
        );
    }
}
