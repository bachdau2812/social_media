package com.dauducbach.clone.modules.media.music.fetch;

import reactor.core.publisher.Mono;

public interface MusicFetchStore {
    Mono<MusicTrack> findById(String trackId);

    Mono<MusicTrack> saveFetched(MusicTrack track, MusicAudioUpload upload);
}
