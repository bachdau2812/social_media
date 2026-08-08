package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchFailedEvent;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
import com.dauducbach.clone.modules.media.service.CloudinaryAudioStorageService;
import com.dauducbach.clone.modules.media.service.MediaAssetCleanupService;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
public class SpotifyMusicFetchService {
    private static final Logger log = LoggerFactory.getLogger(SpotifyMusicFetchService.class);
    private static final Pattern SPOTIFY_TRACK_ID = Pattern.compile("[A-Za-z0-9]{22}");
    private static final String SUCCESS_EVENT = "music_fetch_success";
    private static final String FAILED_EVENT = "music_fetch_failed";
    private static final String SAFE_FAILURE_MESSAGE =
            "Kh\u00f4ng th\u1ec3 t\u1ea3i b\u00e0i h\u00e1t. Vui l\u00f2ng th\u1eed l\u1ea1i.";
    private final MusicsRepository repository;
    private final SpotifyMusicFetchLock lock;
    private final SpotifyOEmbedClient oEmbedClient;
    private final SpotiFlacCliClient spotiFlacClient;
    private final FfprobeMetadataReader metadataReader;
    private final CloudinaryAudioStorageService audioStorage;
    private final MediaService mediaService;
    private final MediaAssetCleanupService cleanupService;
    private final UserSsePublisher ssePublisher;
    private final ObjectMapper objectMapper;
    private final TransactionalOperator transactionalOperator;
    private final SpotifyMusicFetchProperties properties;
    private final Scheduler scheduler;
    private final ConcurrentHashMap<String, Set<String>> waitersByTrack =
            new ConcurrentHashMap<>();

    public SpotifyMusicFetchService(
            MusicsRepository repository,
            SpotifyMusicFetchLock lock,
            SpotifyOEmbedClient oEmbedClient,
            SpotiFlacCliClient spotiFlacClient,
            FfprobeMetadataReader metadataReader,
            CloudinaryAudioStorageService audioStorage,
            MediaService mediaService,
            MediaAssetCleanupService cleanupService,
            UserSsePublisher ssePublisher,
            ObjectMapper objectMapper,
            TransactionalOperator transactionalOperator,
            SpotifyMusicFetchProperties properties,
            @Qualifier("spotifyMusicFetchScheduler") Scheduler scheduler) {
        this.repository = repository;
        this.lock = lock;
        this.oEmbedClient = oEmbedClient;
        this.spotiFlacClient = spotiFlacClient;
        this.metadataReader = metadataReader;
        this.audioStorage = audioStorage;
        this.mediaService = mediaService;
        this.cleanupService = cleanupService;
        this.ssePublisher = ssePublisher;
        this.objectMapper = objectMapper;
        this.transactionalOperator = transactionalOperator;
        this.properties = properties;
        this.scheduler = scheduler;
    }

    public Mono<MusicFetchAcceptedResponse> requestFetch(String trackId, String userId) {
        if (trackId == null || !SPOTIFY_TRACK_ID.matcher(trackId).matches()) {
            return Mono.error(new IllegalArgumentException(
                    "Spotify trackId must contain 22 base-62 characters"));
        }
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(
                    ErrorCode.AUTHENTICATION_FAILED,
                    "Authenticated user is required"));
        }

        String cleanUserId = userId.trim();
        return repository.findById(trackId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.MUSIC_NOT_FOUND)))
                .flatMap(music -> Boolean.TRUE.equals(music.getFetched())
                        ? emitImmediateSuccess(cleanUserId, music)
                                .thenReturn(new MusicFetchAcceptedResponse(
                                        trackId,
                                        MusicFetchAcceptedResponse.Status.ALREADY_FETCHED))
                        : requestUnfetched(trackId, cleanUserId));
    }

    private Mono<MusicFetchAcceptedResponse> requestUnfetched(
            String trackId,
            String userId) {
        registerWaiter(trackId, userId);
        String token = UUID.randomUUID().toString();
        return lock.tryAcquire(trackId, token)
                .flatMap(acquired -> {
                    if (!Boolean.TRUE.equals(acquired)) {
                        return Mono.just(new MusicFetchAcceptedResponse(
                                trackId,
                                MusicFetchAcceptedResponse.Status.PROCESSING));
                    }
                    Path jobDirectory = properties.resolvedTempRoot()
                            .resolve(trackId + "-" + token)
                            .normalize();
                    try {
                        scheduler.schedule(() ->
                                executeJob(trackId, token, jobDirectory)
                                        .subscribe(
                                                unused -> { },
                                                error -> log.error(
                                                        "|SpotifyMusicFetchService|job|terminal error|trackId={}|error={}",
                                                        trackId,
                                                        error.getMessage())));
                    } catch (RuntimeException schedulingError) {
                        unregisterWaiter(trackId, userId);
                        return lock.release(trackId, token)
                                .onErrorResume(error -> Mono.just(false))
                                .then(Mono.error(new AppException(
                                        ErrorCode.MUSIC_FETCH_UNAVAILABLE,
                                        "Spotify music fetch queue is full",
                                        schedulingError)));
                    }
                    return Mono.just(new MusicFetchAcceptedResponse(
                            trackId,
                            MusicFetchAcceptedResponse.Status.STARTED));
                })
                .doOnError(error -> unregisterWaiter(trackId, userId));
    }

    private Mono<Void> executeJob(
            String trackId,
            String token,
            Path jobDirectory) {
        Mono<Musics> fetch = Mono.fromCallable(() -> Files.createDirectories(jobDirectory))
                .then(repository.findById(trackId)
                        .switchIfEmpty(Mono.error(new AppException(ErrorCode.MUSIC_NOT_FOUND))))
                .flatMap(music -> resolveThumbnail(music)
                        .then(spotiFlacClient.download(trackId, jobDirectory))
                        .flatMap(flacFile -> metadataReader.read(flacFile)
                                .flatMap(metadata -> audioStorage.uploadMusic(flacFile, trackId)
                                        .flatMap(upload -> persistFetchedMusic(
                                                music,
                                                metadata,
                                                upload)
                                                .onErrorResume(error -> cleanupUploadedAsset(
                                                        upload,
                                                        error))))));

        Mono<Void> outcome = fetch
                .flatMap(this::publishSuccess)
                .onErrorResume(error -> {
                    log.error(
                            "|SpotifyMusicFetchService|job|failed|trackId={}|error={}",
                            trackId,
                            error.getMessage());
                    return publishFailure(trackId);
                });

        return outcome
                .then(finalizeJob(trackId, token, jobDirectory))
                .onErrorResume(error -> {
                    log.warn(
                            "|SpotifyMusicFetchService|job|finalization failed|trackId={}|error={}",
                            trackId,
                            error.getMessage());
                    waitersByTrack.remove(trackId);
                    return Mono.empty();
                });
    }

    private Mono<Void> resolveThumbnail(Musics music) {
        if (!isBlank(music.getDisplayImages())) {
            return Mono.empty();
        }
        return oEmbedClient.fetchThumbnail(music.getId())
                .doOnNext(music::setDisplayImages)
                .onErrorResume(error -> {
                    log.warn(
                            "|SpotifyMusicFetchService|oembed|best effort failed|trackId={}|error={}",
                            music.getId(),
                            error.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Musics> persistFetchedMusic(
            Musics music,
            SpotifyMusicMetadata metadata,
            MediaAudioUploadResult upload) {
        String songUrl = firstNonBlank(upload.secureUrl(), upload.url());
        if (songUrl == null) {
            return Mono.error(new AppException(
                    ErrorCode.MUSIC_FETCH_FAILED,
                    "Cloudinary upload result is missing a delivery URL"));
        }

        if (!isBlank(metadata.genre())) {
            music.setCategory(metadata.genre());
        }
        music.setDescriptions(metadata.descriptionsJson());
        music.setSongUrl(songUrl);
        music.setFetched(true);

        Mono<Musics> databaseWork = mediaService.saveFetchedMusicMedia(
                        music.getId(),
                        music.getDisplayName(),
                        upload)
                .then(repository.save(music));
        return transactionalOperator.transactional(databaseWork);
    }

    private Mono<Musics> cleanupUploadedAsset(
            MediaAudioUploadResult upload,
            Throwable originalError) {
        return cleanupService.delete(upload.publicId())
                .then(Mono.error(originalError));
    }

    private Mono<Void> emitImmediateSuccess(String userId, Musics music) {
        return serialize(music)
                .flatMap(payload -> ssePublisher.sendToUser(
                        userId,
                        SUCCESS_EVENT,
                        payload));
    }

    private Mono<Void> publishSuccess(Musics music) {
        return serialize(music)
                .flatMap(payload -> notifyWaiters(
                        music.getId(),
                        SUCCESS_EVENT,
                        payload));
    }

    private Mono<Void> publishFailure(String trackId) {
        return serialize(new MusicFetchFailedEvent(trackId, SAFE_FAILURE_MESSAGE))
                .onErrorResume(error -> {
                    log.error(
                            "|SpotifyMusicFetchService|serialize failure event|trackId={}|error={}",
                            trackId,
                            error.getMessage());
                    return Mono.just(
                            "{\"trackId\":\"" + trackId
                                    + "\",\"message\":\""
                                    + SAFE_FAILURE_MESSAGE
                                    + "\"}");
                })
                .flatMap(payload -> notifyWaiters(
                        trackId,
                        FAILED_EVENT,
                        payload));
    }

    private Mono<Void> notifyWaiters(
            String trackId,
            String event,
            String payload) {
        Set<String> waiters = takeWaiters(trackId);
        return Flux.fromIterable(waiters)
                .flatMap(userId -> ssePublisher.sendToUser(userId, event, payload)
                        .onErrorResume(error -> {
                            log.warn(
                                    "|SpotifyMusicFetchService|sse failed|trackId={}|userId={}|event={}|error={}",
                                    trackId,
                                    userId,
                                    event,
                                    error.getMessage());
                            return Mono.empty();
                        }))
                .then();
    }

    private Mono<String> serialize(Object payload) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(payload));
    }

    private Mono<Void> finalizeJob(
            String trackId,
            String token,
            Path jobDirectory) {
        return Mono.defer(() -> {
            waitersByTrack.remove(trackId);
            Mono<Void> tempCleanup = Mono.<Void>fromRunnable(
                            () -> deleteJobDirectory(jobDirectory))
                    .subscribeOn(scheduler)
                    .onErrorResume(error -> {
                        log.warn(
                                "|SpotifyMusicFetchService|temp cleanup failed|trackId={}|error={}",
                                trackId,
                                error.getMessage());
                        return Mono.empty();
                    });
            Mono<Void> lockRelease = lock.release(trackId, token)
                    .doOnNext(released -> {
                        if (!released) {
                            log.warn(
                                    "|SpotifyMusicFetchService|lock token no longer owns key|trackId={}",
                                    trackId);
                        }
                    })
                    .onErrorResume(error -> {
                        log.warn(
                                "|SpotifyMusicFetchService|lock release failed|trackId={}|error={}",
                                trackId,
                                error.getMessage());
                        return Mono.empty();
                    })
                    .then();
            return tempCleanup.then(lockRelease);
        });
    }

    private void deleteJobDirectory(Path jobDirectory) {
        Path root = properties.resolvedTempRoot();
        Path normalizedJob = jobDirectory.toAbsolutePath().normalize();
        if (normalizedJob.equals(root) || !normalizedJob.startsWith(root)) {
            throw new IllegalStateException(
                    "Refusing to delete a path outside the Spotify temp root");
        }
        if (!Files.exists(normalizedJob)) {
            return;
        }
        try (Stream<Path> paths = Files.walk(normalizedJob)) {
            paths.sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (Exception error) {
                            throw new IllegalStateException(
                                    "Could not delete temporary music path " + path,
                                    error);
                        }
                    });
        } catch (Exception error) {
            if (error instanceof IllegalStateException stateError) {
                throw stateError;
            }
            throw new IllegalStateException(
                    "Could not clean temporary music directory",
                    error);
        }
    }

    private void registerWaiter(String trackId, String userId) {
        waitersByTrack.computeIfAbsent(
                        trackId,
                        ignored -> ConcurrentHashMap.newKeySet())
                .add(userId);
    }

    private void unregisterWaiter(String trackId, String userId) {
        waitersByTrack.computeIfPresent(trackId, (ignored, waiters) -> {
            waiters.remove(userId);
            return waiters.isEmpty() ? null : waiters;
        });
    }

    private Set<String> takeWaiters(String trackId) {
        Set<String> waiters = waitersByTrack.remove(trackId);
        return waiters == null ? Set.of() : Set.copyOf(waiters);
    }

    private String firstNonBlank(String preferred, String fallback) {
        if (!isBlank(preferred)) {
            return preferred.trim();
        }
        return isBlank(fallback) ? null : fallback.trim();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
