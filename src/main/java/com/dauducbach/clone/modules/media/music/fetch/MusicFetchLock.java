package com.dauducbach.clone.modules.media.music.fetch;

import reactor.core.publisher.Mono;

public interface MusicFetchLock {
    Mono<Boolean> tryAcquire(String trackId, String token);

    Mono<Boolean> extend(String trackId, String token);

    Mono<Boolean> release(String trackId, String token);
}
