package com.dauducbach.clone.modules.media.publicapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface MusicCatalog {
    Flux<MusicTrackView> findAllByIds(Iterable<String> musicIds);

    Mono<MusicTrackView> findById(String musicId);

    Mono<MusicTrackView> findBySlugNameAndDisplayName(String slugName, String displayName);

    Mono<MusicTrackView> findBySlugName(String slugName);
}
