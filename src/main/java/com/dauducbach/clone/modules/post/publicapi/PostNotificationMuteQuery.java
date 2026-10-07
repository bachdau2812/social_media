package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Mono;

/** Read contract used by notification delivery to honor a post owner's mute preference. */
public interface PostNotificationMuteQuery {
    Mono<Boolean> isMuted(String postId, String userId);
}
