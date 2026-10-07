package com.dauducbach.clone.modules.post.comments.infrastructure.redis;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RedisCommentUploadWaitStoreTest {
    @Mock ReactiveRedisTemplate<String, String> redis;
    @Mock ReactiveValueOperations<String, String> values;

    @Test
    void recordsTheExistingUploadWaitKeyAndLifetime() {
        when(redis.opsForValue()).thenReturn(values);
        when(values.set("wait_for_upload_comment:comment-1", "user-1", RedisCommentUploadWaitStore.TTL))
                .thenReturn(Mono.just(true));

        StepVerifier.create(new RedisCommentUploadWaitStore(redis).markWaitingForUpload("comment-1", "user-1"))
                .verifyComplete();

        verify(values).set("wait_for_upload_comment:comment-1", "user-1", RedisCommentUploadWaitStore.TTL);
    }
}
