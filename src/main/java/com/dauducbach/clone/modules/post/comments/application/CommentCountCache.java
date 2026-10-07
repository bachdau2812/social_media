package com.dauducbach.clone.modules.post.comments.application;

import reactor.core.publisher.Mono;

import java.util.function.Supplier;

/** Cache boundary for the post comment count, including its per-key coordination. */
public interface CommentCountCache {
    Mono<Long> getPostCount(String postId, Supplier<Mono<Long>> loadFromStore);

    Mono<Void> invalidatePostCount(String postId);

    Mono<Void> adjustPostCount(String postId, long delta);
}
