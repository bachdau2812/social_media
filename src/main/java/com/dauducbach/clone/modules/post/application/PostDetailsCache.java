package com.dauducbach.clone.modules.post.application;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import reactor.core.publisher.Mono;

import java.util.List;

public interface PostDetailsCache {
    Mono<PostDetails> get(String postId);

    Mono<Boolean> put(PostDetails post);

    Mono<Void> evict(String postId);

    Mono<Void> evictAll(List<String> postIds);
}
