package com.dauducbach.clone.modules.media.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.Uploader;
import com.cloudinary.Url;
import org.mockito.Answers;
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
        Url httpDelivery = mock(Url.class, Answers.RETURNS_SELF);
        Url secureDelivery = mock(Url.class, Answers.RETURNS_SELF);
        when(cloudinary.uploader()).thenReturn(uploader);
        when(cloudinary.url()).thenReturn(httpDelivery, secureDelivery);
        when(httpDelivery.generate("social_network_musics/1Gqm6KaobG2A1mFVjGnJsS"))
                .thenReturn("http://res.cloudinary.test/video/upload/v42/song.mp3");
        when(secureDelivery.generate("social_network_musics/1Gqm6KaobG2A1mFVjGnJsS"))
                .thenReturn("https://res.cloudinary.test/video/upload/v42/song.mp3");
        when(uploader.upload(any(), any(Map.class))).thenReturn(Map.of(
                "asset_id", "asset-1",
                "public_id", "social_network_musics/1Gqm6KaobG2A1mFVjGnJsS",
                "format", "flac",
                "resource_type", "video",
                "bytes", 1234,
                "version", 42,
                "url", "http://res.cloudinary.test/song.flac",
                "secure_url", "https://res.cloudinary.test/song.flac"));
        Path file = Files.writeString(tempDirectory.resolve("song.flac"), "audio");

        StepVerifier.create(new CloudinaryAudioStorageService(cloudinary)
                        .uploadMusic(file, "1Gqm6KaobG2A1mFVjGnJsS"))
                .assertNext(result -> {
                    assertThat(result.assetId()).isEqualTo("asset-1");
                    assertThat(result.url()).isEqualTo("http://res.cloudinary.test/video/upload/v42/song.mp3");
                    assertThat(result.secureUrl()).isEqualTo("https://res.cloudinary.test/video/upload/v42/song.mp3");
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
        verify(httpDelivery).resourceType("video");
        verify(httpDelivery).type("upload");
        verify(httpDelivery).version("42");
        verify(httpDelivery).format("mp3");
        verify(httpDelivery).secure(false);
        verify(secureDelivery).secure(true);
    }
}
