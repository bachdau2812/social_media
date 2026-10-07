package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Mono;

/** Consumer-owned port for the author context used when building post vectors. */
public interface PostAuthorPreferenceQuery {
    Mono<PostAuthorPreferenceContext> loadPostAuthorContext(String userId);
}
