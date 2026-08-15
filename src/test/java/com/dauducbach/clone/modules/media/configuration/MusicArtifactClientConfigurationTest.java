package com.dauducbach.clone.modules.media.configuration;

import com.dauducbach.clone.modules.media.dto.music.internal.MusicArtifactDescriptor;
import com.dauducbach.clone.modules.media.service.music.SpotifyMusicMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class MusicArtifactClientConfigurationTest {
    @Test
    void exposesSafeDefaultsForThePythonArtifactService() {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();

        assertThat(properties.getServiceBaseUrl()).isEqualTo("http://127.0.0.1:8000");
        assertThat(properties.getServiceTimeout()).isEqualTo(Duration.ofMinutes(6));
        assertThat(properties.getArtifactMaxSize()).isEqualTo(DataSize.ofMegabytes(100));
    }

    @Test
    void mapsTheCamelCasePythonArtifactContract() throws Exception {
        String json = """
                {
                  "artifactId":"5f8a0df0-695d-48ef-98fc-24883ba8b61b",
                  "trackId":"1Gqm6KaobG2A1mFVjGnJsS",
                  "filename":"1Gqm6KaobG2A1mFVjGnJsS.flac",
                  "contentType":"audio/flac",
                  "sizeBytes":5,
                  "sha256":"6ed8919ce20490a5e3ad8630a4fab69475297abd07db73918dd5f36fcfaeb11b",
                  "expiresAt":"2026-08-15T10:30:00Z",
                  "metadata":{"genre":"Rock","albumArtist":"Artist"}
                }
                """;
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

        MusicArtifactDescriptor value = objectMapper.readValue(json, MusicArtifactDescriptor.class);

        assertThat(value.artifactId()).isEqualTo("5f8a0df0-695d-48ef-98fc-24883ba8b61b");
        assertThat(value.expiresAt()).isEqualTo(Instant.parse("2026-08-15T10:30:00Z"));
        assertThat(value.metadata().albumArtist()).isEqualTo("Artist");
        assertThat(value.metadata().genre()).isEqualTo("Rock");
        assertThat(value.metadata().toSpotifyMetadata())
                .isEqualTo(new SpotifyMusicMetadata(null, null, null, "Artist", null, "Rock", null));
    }

    @Test
    void registersADedicatedQualifiedWebClient() {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.registerBean(SpotifyMusicFetchProperties.class, () -> properties);
        context.register(MusicArtifactClientConfiguration.class);
        context.refresh();

        try {
            assertThat(context.getBean("musicArtifactWebClient", WebClient.class)).isNotNull();
        } finally {
            context.close();
        }
    }
}
