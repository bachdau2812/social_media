package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.dauducbach.clone.modules.media.dto.music.internal.MusicArtifactDescriptor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class MusicArtifactClient {
    private static final String ARTIFACT_COLLECTION_PATH = "/api/v1/music/artifacts";
    private static final int ERROR_BODY_MAX_BYTES = 64 * 1024;
    private static final String SAFE_LOG_VALUE = "[A-Za-z0-9._:-]{1,128}";
    private static final ObjectMapper ERROR_MAPPER = new ObjectMapper();

    private final WebClient webClient;
    private final SpotifyMusicFetchProperties properties;

    public MusicArtifactClient(
            @Qualifier("musicArtifactWebClient") WebClient webClient,
            SpotifyMusicFetchProperties properties) {
        this.webClient = webClient;
        this.properties = properties;
    }

    public Mono<MusicArtifactDescriptor> create(String trackId) {
        return webClient.post()
                .uri(ARTIFACT_COLLECTION_PATH)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(new CreateArtifactRequest(trackId))
                .exchangeToMono(response -> {
                    if (response.statusCode().value() == 201) {
                        return response.bodyToMono(MusicArtifactDescriptor.class)
                                .switchIfEmpty(Mono.error(fetchFailure(
                                        "Music artifact service returned an empty descriptor", null)));
                    }
                    return serviceError(response, "create");
                })
                .timeout(properties.getServiceTimeout())
                .onErrorMap(this::mapFailure);
    }

    public Mono<DownloadedMusicArtifact> download(
            MusicArtifactDescriptor descriptor,
            Path jobDirectory) {
        return Mono.defer(() -> {
                    String artifactId = validatedArtifactId(descriptor);
                    validateDescriptorSize(descriptor);
                    return prepareDestination(jobDirectory)
                            .flatMap(destination -> downloadTo(
                                            artifactId,
                                            descriptor,
                                            destination)
                                    .timeout(properties.getServiceTimeout())
                                    .onErrorResume(error -> deletePartial(destination)
                                            .then(Mono.error(mapFailure(error)))));
                })
                .onErrorMap(this::mapFailure);
    }

    public Mono<Void> cleanup(MusicArtifactDescriptor descriptor) {
        return Mono.defer(() -> {
                    String artifactId = validatedArtifactId(descriptor);
                    return webClient.delete()
                            .uri(uriBuilder -> uriBuilder
                                    .pathSegment("api", "v1", "music", "artifacts", artifactId)
                                    .build())
                            .exchangeToMono(response -> {
                                int status = response.statusCode().value();
                                if (status == 204 || status == 404) {
                                    return response.releaseBody();
                                }
                                return serviceError(response, "cleanup");
                            });
                })
                .timeout(properties.getServiceTimeout())
                .onErrorResume(error -> {
                    log.warn(
                            "|MusicArtifactClient|cleanup|suppressed|artifactId={}|errorType={}",
                            safeArtifactId(descriptor),
                            error.getClass().getSimpleName());
                    return Mono.empty();
                });
    }

    private Mono<DownloadedMusicArtifact> downloadTo(
            String artifactId,
            MusicArtifactDescriptor descriptor,
            Path destination) {
        long maxBytes = properties.getArtifactMaxSize().toBytes();
        AtomicLong transferred = new AtomicLong();

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .pathSegment("api", "v1", "music", "artifacts", artifactId, "audio")
                        .build())
                .exchangeToMono(response -> {
                    if (response.statusCode().value() != 200) {
                        return serviceError(response, "download");
                    }
                    long contentLength = response.headers().contentLength().orElse(-1L);
                    if (contentLength == 0 || contentLength > maxBytes) {
                        return Mono.error(fetchFailure(
                                "Music artifact response size is outside the configured limit", null));
                    }

                    Flux<DataBuffer> boundedBody = response.bodyToFlux(DataBuffer.class)
                            .<DataBuffer>handle((buffer, sink) -> {
                                long total = transferred.addAndGet(buffer.readableByteCount());
                                if (total > maxBytes) {
                                    DataBufferUtils.release(buffer);
                                    sink.error(fetchFailure(
                                            "Music artifact response exceeded the configured limit", null));
                                    return;
                                }
                                sink.next(buffer);
                            })
                            .doOnDiscard(DataBuffer.class, DataBufferUtils::release);

                    return DataBufferUtils.write(
                                    boundedBody,
                                    destination,
                                    StandardOpenOption.CREATE_NEW,
                                    StandardOpenOption.WRITE)
                            .then(Mono.defer(() -> verifyDownloadedArtifact(
                                    descriptor,
                                    destination,
                                    transferred.get())))
                            .thenReturn(new DownloadedMusicArtifact(descriptor, destination));
                });
    }

    private Mono<Void> verifyDownloadedArtifact(
            MusicArtifactDescriptor descriptor,
            Path destination,
            long transferredBytes) {
        if (transferredBytes != descriptor.sizeBytes()) {
            return Mono.error(fetchFailure("Music artifact size verification failed", null));
        }

        return Mono.fromCallable(() -> sha256(destination))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(actualHash -> {
                    byte[] expectedHash;
                    try {
                        expectedHash = HexFormat.of().parseHex(descriptor.sha256());
                    } catch (RuntimeException error) {
                        return Mono.error(fetchFailure("Music artifact SHA-256 is invalid", error));
                    }
                    if (!MessageDigest.isEqual(actualHash, expectedHash)) {
                        return Mono.error(fetchFailure("Music artifact SHA-256 verification failed", null));
                    }
                    return Mono.empty();
                });
    }

    private Mono<Path> prepareDestination(Path jobDirectory) {
        return Mono.fromCallable(() -> {
                    Files.createDirectories(jobDirectory);
                    return jobDirectory.resolve(UUID.randomUUID() + ".flac");
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    private Mono<Void> deletePartial(Path destination) {
        return Mono.fromRunnable(() -> {
                    try {
                        Files.deleteIfExists(destination);
                    } catch (Exception cleanupError) {
                        log.warn(
                                "|MusicArtifactClient|deletePartial|suppressed|file={}|errorType={}",
                                destination.getFileName(),
                                cleanupError.getClass().getSimpleName());
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    private <T> Mono<T> serviceError(ClientResponse response, String operation) {
        HttpStatusCode status = response.statusCode();
        String headerRequestId = response.headers().header("X-Request-ID").stream()
                .findFirst()
                .map(this::safeLogValue)
                .orElse("");
        return readSafeProblem(response)
                .flatMap(problem -> {
                    String requestId = problem.requestId().isBlank()
                            ? headerRequestId
                            : problem.requestId();
                    log.warn(
                            "|MusicArtifactClient|{}|failed|status={}|code={}|requestId={}",
                            operation,
                            status.value(),
                            problem.code(),
                            requestId);
                    String detail = "Music artifact service returned HTTP " + status.value()
                            + safeSuffix("code", problem.code())
                            + safeSuffix("requestId", requestId);
                    return Mono.error(fetchFailure(detail, null));
                });
    }

    private Mono<SafeProblem> readSafeProblem(ClientResponse response) {
        return DataBufferUtils.join(response.bodyToFlux(DataBuffer.class), ERROR_BODY_MAX_BYTES)
                .map(buffer -> {
                    byte[] bytes = new byte[buffer.readableByteCount()];
                    try {
                        buffer.read(bytes);
                    } finally {
                        DataBufferUtils.release(buffer);
                    }
                    return parseSafeProblem(bytes);
                })
                .defaultIfEmpty(SafeProblem.EMPTY)
                .onErrorReturn(SafeProblem.EMPTY);
    }

    private SafeProblem parseSafeProblem(byte[] bytes) {
        try {
            JsonNode error = ERROR_MAPPER.readTree(bytes).path("error");
            return new SafeProblem(
                    safeText(error.path("code")),
                    safeText(error.path("requestId")));
        } catch (Exception ignored) {
            return SafeProblem.EMPTY;
        }
    }

    private String safeText(JsonNode value) {
        return value.isTextual() ? safeLogValue(value.textValue()) : "";
    }

    private String safeLogValue(String value) {
        return value != null && value.matches(SAFE_LOG_VALUE) ? value : "";
    }

    private String safeSuffix(String name, String value) {
        return value == null || value.isBlank() ? "" : " (" + name + "=" + value + ")";
    }

    private void validateDescriptorSize(MusicArtifactDescriptor descriptor) {
        long maxBytes = properties.getArtifactMaxSize().toBytes();
        if (descriptor.sizeBytes() <= 0 || descriptor.sizeBytes() > maxBytes) {
            throw fetchFailure("Music artifact descriptor size is outside the configured limit", null);
        }
    }

    private String validatedArtifactId(MusicArtifactDescriptor descriptor) {
        if (descriptor == null || descriptor.artifactId() == null) {
            throw fetchFailure("Music artifact descriptor is invalid", null);
        }
        try {
            return UUID.fromString(descriptor.artifactId()).toString();
        } catch (IllegalArgumentException error) {
            throw fetchFailure("Music artifact ID is invalid", error);
        }
    }

    private String safeArtifactId(MusicArtifactDescriptor descriptor) {
        if (descriptor == null || descriptor.artifactId() == null) {
            return "unknown";
        }
        try {
            return UUID.fromString(descriptor.artifactId()).toString();
        } catch (IllegalArgumentException ignored) {
            return "invalid";
        }
    }

    private byte[] sha256(Path file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = Files.newInputStream(file)) {
            byte[] buffer = new byte[8192];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return digest.digest();
    }

    private Throwable mapFailure(Throwable error) {
        return error instanceof AppException
                ? error
                : fetchFailure("Music artifact service request failed", error);
    }

    private AppException fetchFailure(String message, Throwable cause) {
        return new AppException(ErrorCode.MUSIC_FETCH_FAILED, message, cause);
    }

    private record CreateArtifactRequest(String trackId) {
    }

    private record SafeProblem(String code, String requestId) {
        private static final SafeProblem EMPTY = new SafeProblem("", "");
    }
}
