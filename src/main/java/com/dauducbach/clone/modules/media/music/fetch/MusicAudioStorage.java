package com.dauducbach.clone.modules.media.music.fetch;

import reactor.core.publisher.Mono;

import java.nio.file.Path;

public interface MusicAudioStorage {
    Mono<MusicAudioUpload> uploadMusic(Path file, String publicId);
}
