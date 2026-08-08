package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component
@RequiredArgsConstructor
public class FfprobeMetadataReader {
    private final CliCommandRunner runner;
    private final SpotifyMusicFetchProperties properties;
    private final ObjectMapper objectMapper;

    public Mono<SpotifyMusicMetadata> read(Path flacFile) {
        if (flacFile == null) {
            return Mono.error(new IllegalArgumentException("FLAC file is required"));
        }
        List<String> command = List.of(
                properties.getFfprobeCommand(),
                "-v", "quiet",
                "-print_format", "json",
                "-show_format",
                "-show_streams",
                flacFile.toString());

        return runner.run(command, properties.getProcessTimeout())
                .flatMap(result -> result.exitCode() == 0
                        ? parse(result.output())
                        : Mono.error(new IllegalStateException(
                                "ffprobe exited with exit code " + result.exitCode() + ": " + result.output())));
    }

    private Mono<SpotifyMusicMetadata> parse(String output) {
        return Mono.fromCallable(() -> {
            JsonNode tagsNode = objectMapper.readTree(output).path("format").path("tags");
            Map<String, String> tags = new HashMap<>();
            if (tagsNode.isObject()) {
                tagsNode.fields().forEachRemaining(entry -> {
                    if (entry.getValue().isValueNode()) {
                        tags.put(
                                entry.getKey().toUpperCase(Locale.ROOT),
                                entry.getValue().asText());
                    }
                });
            }
            return new SpotifyMusicMetadata(
                    normalize(tags.get("TITLE")),
                    normalize(tags.get("ARTIST")),
                    normalize(tags.get("ALBUM")),
                    normalize(tags.get("ALBUM_ARTIST")),
                    normalize(tags.get("COMPOSER")),
                    normalize(tags.get("GENRE")),
                    normalize(tags.get("LYRICS")));
        });
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
