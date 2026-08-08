package com.dauducbach.clone.modules.media.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.Uploader;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import reactor.test.StepVerifier;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CloudinaryAudioStorageServiceTest {
    @TempDir
    Path tempDirectory;

    @Test
    void uploadsTheFlacAsAFileWithStableMusicOptions() throws Exception {
        Cloudinary cloudinary = mock(Cloudinary.class);
        Uploader uploader = mock(Uploader.class);
        when(cloudinary.uploader()).thenReturn(uploader);
        when(uploader.upload(any(), any(Map.class))).thenReturn(Map.of(
                "asset_id", "asset-1",
                "public_id", "social_network_musics/1Gqm6KaobG2A1mFVjGnJsS",
                "format", "flac",
                "resource_type", "video",
                "bytes", 1234,
                "url", "http://res.cloudinary.test/song.flac",
                "secure_url", "https://res.cloudinary.test/song.flac"));
        Path file = Files.writeString(tempDirectory.resolve("song.flac"), "audio");

        StepVerifier.create(new CloudinaryAudioStorageService(cloudinary)
                        .uploadMusic(file, "1Gqm6KaobG2A1mFVjGnJsS"))
                .assertNext(result -> {
                    assertThat(result.assetId()).isEqualTo("asset-1");
                    assertThat(result.secureUrl()).isEqualTo("https://res.cloudinary.test/song.flac");
                })
                .verifyComplete();

        ArgumentCaptor<Object> uploadSource = ArgumentCaptor.forClass(Object.class);
        ArgumentCaptor<Map<String, Object>> options = ArgumentCaptor.forClass(Map.class);
        verify(uploader).upload(uploadSource.capture(), options.capture());
        assertThat(uploadSource.getValue()).isInstanceOf(File.class);
        assertThat(((File) uploadSource.getValue()).toPath()).isEqualTo(file);
        assertThat(options.getValue()).containsEntry("resource_type", "video")
                .containsEntry("folder", "social_network_musics")
                .containsEntry("public_id", "1Gqm6KaobG2A1mFVjGnJsS")
                .containsEntry("overwrite", true);
    }
}
