package com.dauducbach.clone.modules.media.music.fetch;

import com.dauducbach.clone.modules.media.dto.music.internal.MusicArtifactDescriptor;
import reactor.core.publisher.Mono;

import java.nio.file.Path;

public interface MusicArtifactGateway {
    Mono<MusicArtifactDescriptor> create(String trackId);

    Mono<DownloadedMusicArtifact> download(MusicArtifactDescriptor descriptor, Path jobDirectory);

    Mono<Void> cleanup(MusicArtifactDescriptor descriptor);
}
