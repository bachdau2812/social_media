package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.dto.music.internal.MusicArtifactDescriptor;

import java.nio.file.Path;

public record DownloadedMusicArtifact(
        MusicArtifactDescriptor descriptor,
        Path file) {
}
