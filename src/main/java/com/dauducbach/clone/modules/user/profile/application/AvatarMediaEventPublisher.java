package com.dauducbach.clone.modules.user.profile.application;

import reactor.core.publisher.Mono;

public interface AvatarMediaEventPublisher {
    Mono<Void> requestAvatarScan(String userId, String avatarUrl, String publicId);

    Mono<Void> publishAvatarUpdated(String userId, String avatarUrl, String mediaId);
}
