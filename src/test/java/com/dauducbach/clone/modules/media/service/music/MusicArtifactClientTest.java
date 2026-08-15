package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.dauducbach.clone.modules.media.dto.music.internal.MusicArtifactDescriptor;
import com.dauducbach.clone.modules.media.dto.music.internal.MusicArtifactMetadata;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpHeaders;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.DisposableServer;
import reactor.netty.http.server.HttpServer;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class MusicArtifactClientTest {
    private static final String TRACK_ID = "2plbrEY59IikOBgBGLjaoe";
    private static final String ARTIFACT_ID = "5f8a0df0-695d-48ef-98fc-24883ba8b61b";
    private static final byte[] AUDIO = "audio".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path tempDirectory;

    private DisposableServer server;

    @AfterEach
    void stopServer() {
        if (server != null) {
            server.disposeNow();
        }
    }

    @Test
    void postsTrackIdAndParsesCreatedDescriptor() {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();
        start((request, response) -> {
            method.set(request.method().name());
            path.set(request.uri());
            return request.receive().aggregate().asString()
                    .doOnNext(body::set)
                    .then(response.status(201)
                            .header(HttpHeaders.CONTENT_TYPE, "application/json")
                            .sendString(reactor.core.publisher.Mono.just(descriptorJson(AUDIO)))
                            .then());
        });

        MusicArtifactDescriptor descriptor = client(DataSize.ofMegabytes(100), Duration.ofSeconds(2))
                .create(TRACK_ID)
                .block();

        assertThat(descriptor).isNotNull();
        assertThat(descriptor.artifactId()).isEqualTo(ARTIFACT_ID);
        assertThat(method).hasValue("POST");
        assertThat(path).hasValue("/api/v1/music/artifacts");
        assertThat(body.get()).isEqualTo("{\"trackId\":\"" + TRACK_ID + "\"}");
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 413, 429, 502, 504})
    void mapsServiceErrorsWithoutExposingRawBody(int status) {
        start((request, response) -> response.status(status)
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .sendString(reactor.core.publisher.Mono.just("""
                        {"error":{"code":"SAFE_CODE","message":"super-secret-internal-output","requestId":"req-7"}}
                        """))
                .then());

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofSeconds(2)).create(TRACK_ID))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    AppException appException = (AppException) error;
                    assertThat(appException.getErrorCode()).isEqualTo(ErrorCode.MUSIC_FETCH_FAILED);
                    assertThat(appException.getDetailMessage())
                            .contains(String.valueOf(status), "SAFE_CODE", "req-7")
                            .doesNotContain("super-secret-internal-output");
                })
                .verify();
    }

    @Test
    void mapsMalformedCreatedJson() {
        start((request, response) -> response.status(201)
                .header(HttpHeaders.CONTENT_TYPE, "application/json")
                .sendString(reactor.core.publisher.Mono.just("{not-json"))
                .then());

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofSeconds(2)).create(TRACK_ID))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode()).isEqualTo(ErrorCode.MUSIC_FETCH_FAILED);
                })
                .verify();
    }

    @Test
    void streamsExactBytesToAUniqueCreateNewFile() throws Exception {
        start((request, response) -> response.status(200)
                .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(AUDIO.length))
                .sendByteArray(reactor.core.publisher.Mono.just(AUDIO))
                .then());
        MusicArtifactClient client = client(DataSize.ofMegabytes(100), Duration.ofSeconds(2));
        MusicArtifactDescriptor descriptor = descriptor(AUDIO);

        DownloadedMusicArtifact first = client.download(descriptor, tempDirectory).block();
        DownloadedMusicArtifact second = client.download(descriptor, tempDirectory).block();

        assertThat(first).isNotNull();
        assertThat(second).isNotNull();
        assertThat(first.file()).isNotEqualTo(second.file());
        assertThat(Files.readAllBytes(first.file())).isEqualTo(AUDIO);
        assertThat(Files.readAllBytes(second.file())).isEqualTo(AUDIO);
    }

    @Test
    void rejectsOversizedContentLengthBeforeConsumingBody() {
        start((request, response) -> response.status(200)
                .header(HttpHeaders.CONTENT_LENGTH, "6")
                .send(Flux.just(Unpooled.wrappedBuffer("123456".getBytes(StandardCharsets.UTF_8))))
                .then());

        StepVerifier.create(client(DataSize.ofBytes(5), Duration.ofSeconds(2))
                        .download(descriptor(AUDIO), tempDirectory))
                .expectError(AppException.class)
                .verify();

        assertThat(tempDirectory).isEmptyDirectory();
    }

    @Test
    void abortsChunkedBodyCrossingLimitAndDeletesPartialFile() {
        start((request, response) -> response.status(200)
                .send(Flux.just(
                        Unpooled.wrappedBuffer("123".getBytes(StandardCharsets.UTF_8)),
                        Unpooled.wrappedBuffer("456".getBytes(StandardCharsets.UTF_8))))
                .then());

        StepVerifier.create(client(DataSize.ofBytes(5), Duration.ofSeconds(2))
                        .download(descriptorWithSizeAndHash(5, sha256(AUDIO)), tempDirectory))
                .expectError(AppException.class)
                .verify();

        assertThat(tempDirectory).isEmptyDirectory();
    }

    @Test
    void rejectsDeclaredSizeMismatchAndDeletesPartialFile() {
        startAudio(AUDIO);

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofSeconds(2))
                        .download(descriptorWithSizeAndHash(AUDIO.length + 1, sha256(AUDIO)), tempDirectory))
                .expectError(AppException.class)
                .verify();

        assertThat(tempDirectory).isEmptyDirectory();
    }

    @Test
    void rejectsSha256MismatchAndDeletesPartialFile() {
        startAudio(AUDIO);

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofSeconds(2))
                        .download(descriptorWithSizeAndHash(AUDIO.length, "0".repeat(64)), tempDirectory))
                .expectError(AppException.class)
                .verify();

        assertThat(tempDirectory).isEmptyDirectory();
    }

    @Test
    void timesOutSlowCreateRequest() {
        start((request, response) -> response.status(201)
                .sendString(reactor.core.publisher.Mono.just(descriptorJson(AUDIO)).delayElement(Duration.ofSeconds(1)))
                .then());

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofMillis(50)).create(TRACK_ID))
                .expectError(AppException.class)
                .verify();
    }

    @Test
    void cleanupCallsEncodedDeleteAndSuppressesHttpFailures() {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        start((request, response) -> {
            method.set(request.method().name());
            path.set(request.uri());
            return response.status(502).send().then();
        });

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofSeconds(2)).cleanup(descriptor(AUDIO)))
                .verifyComplete();

        assertThat(method).hasValue("DELETE");
        assertThat(path).hasValue("/api/v1/music/artifacts/" + ARTIFACT_ID);
    }

    @Test
    void cleanupTreatsNotFoundAsSuccess() {
        start((request, response) -> response.status(404).send().then());

        StepVerifier.create(client(DataSize.ofMegabytes(100), Duration.ofSeconds(2)).cleanup(descriptor(AUDIO)))
                .verifyComplete();
    }

    private void startAudio(byte[] bytes) {
        start((request, response) -> response.status(200)
                .sendByteArray(reactor.core.publisher.Mono.just(bytes))
                .then());
    }

    private void start(java.util.function.BiFunction<
            reactor.netty.http.server.HttpServerRequest,
            reactor.netty.http.server.HttpServerResponse,
            ? extends org.reactivestreams.Publisher<Void>> handler) {
        server = HttpServer.create().host("127.0.0.1").port(0).handle(handler).bindNow();
    }

    private MusicArtifactClient client(DataSize maxSize, Duration timeout) {
        SpotifyMusicFetchProperties properties = new SpotifyMusicFetchProperties();
        properties.setArtifactMaxSize(maxSize);
        properties.setServiceTimeout(timeout);
        WebClient webClient = WebClient.builder()
                .baseUrl("http://127.0.0.1:" + server.port())
                .build();
        return new MusicArtifactClient(webClient, properties);
    }

    private MusicArtifactDescriptor descriptor(byte[] bytes) {
        return descriptorWithSizeAndHash(bytes.length, sha256(bytes));
    }

    private MusicArtifactDescriptor descriptorWithSizeAndHash(long size, String hash) {
        return new MusicArtifactDescriptor(
                ARTIFACT_ID,
                TRACK_ID,
                TRACK_ID + ".flac",
                "audio/flac",
                size,
                hash,
                Instant.parse("2026-08-15T10:30:00Z"),
                new MusicArtifactMetadata("Title", "Artist", null, null, null, null, null));
    }

    private String descriptorJson(byte[] bytes) {
        return """
                {
                  "artifactId":"%s",
                  "trackId":"%s",
                  "filename":"%s.flac",
                  "contentType":"audio/flac",
                  "sizeBytes":%d,
                  "sha256":"%s",
                  "expiresAt":"2026-08-15T10:30:00Z",
                  "metadata":{"title":"Title","artist":"Artist"}
                }
                """.formatted(ARTIFACT_ID, TRACK_ID, TRACK_ID, bytes.length, sha256(bytes));
    }

    private static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }
}
