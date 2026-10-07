package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CommentQuery {
    Mono<CommentSnapshot> findById(String commentId);

    Flux<String> findDistinctCommenterUserIdsByPostId(String postId);

    Mono<Long> countByPostId(String postId);

    record CommentSnapshot(String id, String postId, String authorId, String parentId, String content) { }
}
