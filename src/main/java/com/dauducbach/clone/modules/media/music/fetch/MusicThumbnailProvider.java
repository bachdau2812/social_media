package com.dauducbach.clone.modules.media.music.fetch;

import reactor.core.publisher.Mono;

public interface MusicThumbnailProvider {
    Mono<String> fetchThumbnail(String trackId);
}
