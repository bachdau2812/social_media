package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FfprobeMetadataReaderTest {
    @Mock
    CliCommandRunner runner;

    @Test
    void readsTagsCaseInsensitivelyAndOmitsMissingDescriptionKeys() {
        SpotifyMusicFetchProperties properties = properties();
        FfprobeMetadataReader reader = new FfprobeMetadataReader(runner, properties, new ObjectMapper());
        String json = """
                {"format":{"tags":{
                  "TITLE":"Song","ARTIST":"Artist","ALBUM":"Album",
                  "album_artist":"Album Artist","COMPOSER":"Composer",
                  "GENRE":"Rap/Hip Hop"
                }}}
                """;
        when(runner.run(anyList(), any())).thenReturn(Mono.just(new CliCommandResult(0, json)));

        StepVerifier.create(reader.read(Path.of("song.flac")))
                .assertNext(metadata -> {
                    assertThat(metadata.title()).isEqualTo("Song");
                    assertThat(metadata.artist()).isEqualTo("Artist");
                    assertThat(metadata.album()).isEqualTo("Album");
                    assertThat(metadata.genre()).isEqualTo("Rap/Hip Hop");
                    assertThat(metadata.descriptionsJson())
                            .isEqualTo("{\"COMPOSER\":\"Composer\",\"ALBUM_ARTIST\":\"Album Artist\"}");
                })
                .verifyComplete();

        ArgumentCaptor<List<String>> command = ArgumentCaptor.forClass(List.class);
        org.mockito.Mockito.verify(runner).run(command.capture(), any(Duration.class));
        assertThat(command.getValue()).containsExactly(
                "ffprobe", "-v", "quiet", "-print_format", "json",
                "-show_format", "-show_streams", "song.flac");
    }

    @Test
    void writesLyricsAfterAlbumArtistAndOmitsMissingComposer() {
        FfprobeMetadataReader reader = new FfprobeMetadataReader(runner, properties(), new ObjectMapper());
        String json = """
                {"format":{"tags":{
                  "ALBUM_ARTIST":"Artist","lyrics":"line one\\nline two"
                }}}
                """;
        when(runner.run(anyList(), any())).thenReturn(Mono.just(new CliCommandResult(0, json)));

        StepVerifier.create(reader.read(Path.of("song.flac")))
                .assertNext(metadata -> assertThat(metadata.descriptionsJson())
                        .isEqualTo("{\"ALBUM_ARTIST\":\"Artist\",\"LYRICS\":\"line one\\nline two\"}"))
                .verifyComplete();
    }

    @Test
    void rejectsNonZeroFfprobeExit() {
        FfprobeMetadataReader reader = new FfprobeMetadataReader(runner, properties(), new ObjectMapper());
        when(runner.run(anyList(), any())).thenReturn(Mono.just(new CliCommandResult(1, "probe failed")));

        StepVerifier.create(reader.read(Path.of("song.flac")))
                .expectErrorMatches(error -> error.getMessage().contains("ffprobe")
                        && error.getMessage().contains("exit code 1"))
                .verify();
    }

    private SpotifyMusicFetchProperties properties() {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();
        properties.setFfprobeCommand("ffprobe");
        properties.setProcessTimeout(Duration.ofMinutes(5));
        return properties;
    }
}
