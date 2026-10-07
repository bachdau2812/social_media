package com.dauducbach.clone.modules.post.application;

import reactor.core.publisher.Mono;

public interface PostNotificationMuteStore {
    Mono<Boolean> mute(String postId, String userId);
}
