package com.dauducbach.clone.modules.media.publicapi;

import reactor.core.publisher.Mono;

public interface MediaInspection {
    Mono<Result> inspect(String mediaUrl, String publicId);

    record Result(boolean nsfw) {
        public boolean approved() {
            return !nsfw;
        }
    }
}
