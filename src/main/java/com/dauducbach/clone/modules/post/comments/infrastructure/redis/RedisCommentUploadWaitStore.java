package com.dauducbach.clone.modules.post.comments.infrastructure.redis;

import com.dauducbach.clone.modules.post.comments.application.CommentUploadWaitStore;
import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisCommentUploadWaitStore implements CommentUploadWaitStore {
    static final String KEY_PREFIX = "wait_for_upload_comment:";
    static final Duration TTL = Duration.ofHours(1);

    private final ReactiveRedisTemplate<String, String> redis;

    @Override
    public Mono<Void> markWaitingForUpload(String commentId, String userId) {
        return redis.opsForValue().set(KEY_PREFIX + commentId, userId, TTL).then();
    }
}
