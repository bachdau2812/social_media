package com.dauducbach.clone.modules.media.music.fetch;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchFailedEvent;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetCleanup;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Scheduler;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@Service
public class FetchMusicUseCase {
    private static final Logger log = LoggerFactory.getLogger(FetchMusicUseCase.class);
    private static final Pattern SPOTIFY_TRACK_ID = Pattern.compile("[A-Za-z0-9]{22}");
    private static final String SUCCESS_EVENT = "music_fetch_success";
    private static final String FAILED_EVENT = "music_fetch_failed";
    private static final String SAFE_FAILURE_MESSAGE =
            "Kh\u00f4ng th\u1ec3 t\u1ea3i b\u00e0i h\u00e1t. Vui l\u00f2ng th\u1eed l\u1ea1i.";
    private final MusicFetchStore musicStore;
    private final FetchMusicWaiters waiters;
    private final MusicFetchLock lock;
    private final MusicThumbnailProvider thumbnailProvider;
    private final MusicArtifactGateway artifactGateway;
    private final MusicAudioStorage audioStorage;
    private final MediaAssetCleanup assetCleanup;
    private final UserSsePublisher ssePublisher;
    private final ObjectMapper objectMapper;
    private final SpotifyMusicFetchProperties properties;
    private final Scheduler jobScheduler;

    public FetchMusicUseCase(
            MusicFetchStore musicStore,
            FetchMusicWaiters waiters,
            MusicFetchLock lock,
            MusicThumbnailProvider thumbnailProvider,
            MusicArtifactGateway artifactGateway,
            MusicAudioStorage audioStorage,
            MediaAssetCleanup assetCleanup,
            UserSsePublisher ssePublisher,
            ObjectMapper objectMapper,
            SpotifyMusicFetchProperties properties,
            @Qualifier("spotifyMusicFetchScheduler") Scheduler jobScheduler) {
        this.musicStore = musicStore;
        this.waiters = waiters;
        this.lock = lock;
        this.thumbnailProvider = thumbnailProvider;
        this.artifactGateway = artifactGateway;
        this.audioStorage = audioStorage;
        this.assetCleanup = assetCleanup;
        this.ssePublisher = ssePublisher;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.jobScheduler = jobScheduler;
    }

    public Mono<MusicFetchAcceptedResponse> requestFetch(String trackId, String userId) {
        if (!isValidTrackId(trackId)) return invalidTrackId();
        if (userId == null || userId.isBlank()) {
            return Mono.error(new AppException(
                    ErrorCode.AUTHENTICATION_FAILED,
                    "Authenticated user is required"));
        }

        String cleanUserId = userId.trim();
                        return musicStore.findById(trackId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.MUSIC_NOT_FOUND)))
                .flatMap(music -> Boolean.TRUE.equals(music.getFetched())
                        ? emitImmediateSuccess(cleanUserId, music)
                                .thenReturn(new MusicFetchAcceptedResponse(
                                        trackId,
                                        MusicFetchAcceptedResponse.Status.ALREADY_FETCHED))
                        : requestUnfetched(trackId, cleanUserId));
    }

    public Mono<MusicFetchAcceptedResponse> requestFetchSilently(String trackId) {
        if (!isValidTrackId(trackId)) return invalidTrackId();

        return musicStore.findById(trackId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.MUSIC_NOT_FOUND)))
                .flatMap(music -> Boolean.TRUE.equals(music.getFetched())
                        ? Mono.just(new MusicFetchAcceptedResponse(
                                trackId,
                                MusicFetchAcceptedResponse.Status.ALREADY_FETCHED))
                        : requestUnfetched(trackId, null));
    }

    private boolean isValidTrackId(String trackId) {
        return trackId != null && SPOTIFY_TRACK_ID.matcher(trackId).matches();
    }

    private Mono<MusicFetchAcceptedResponse> invalidTrackId() {
        return Mono.error(new AppException(
                ErrorCode.MUSIC_REQUEST_INVALID,
                "Spotify trackId must contain 22 base-62 characters"));
    }

    private Mono<MusicFetchAcceptedResponse> requestUnfetched(
            String trackId,
            String userId) {
        FetchMusicWaiters.Registration registration = userId == null
                ? null
                : waiters.register(trackId, userId);
        if (registration != null && registration.terminal() != null) {
            return sendTerminal(userId, registration.terminal())
                    .thenReturn(new MusicFetchAcceptedResponse(
                            trackId,
                            MusicFetchAcceptedResponse.Status.PROCESSING));
        }

        String token = UUID.randomUUID().toString();
        return lock.tryAcquire(trackId, token)
                .flatMap(acquired -> {
                    if (!Boolean.TRUE.equals(acquired)) {
        return musicStore.findById(trackId)
                                .flatMap(latest -> Boolean.TRUE.equals(latest.getFetched())
                                        ? completeAlreadyFetchedRequest(
                                                trackId,
                                                registration,
                                                latest)
                                        : Mono.just(new MusicFetchAcceptedResponse(
                                                trackId,
                                                MusicFetchAcceptedResponse.Status.PROCESSING)));
                    }
                    FetchMusicWaiters.NotificationState notifications = registration == null
                            ? waiters.state(trackId)
                            : registration.state();
                    Path jobDirectory = properties.resolvedTempRoot()
                            .resolve(trackId + "-" + token)
                            .normalize();
                    String jobId = jobDirectory.getFileName().toString();
                    log.info(
                            "[music-fetch] trackId={} jobId={} lock acquired",
                            trackId,
                            jobId);
                    LockHeartbeat heartbeat = startLockHeartbeat(trackId, token);
                    try {
                        jobScheduler.schedule(() -> executeJob(
                                trackId,
                                token,
                                jobDirectory,
                                notifications,
                                heartbeat).block());
                        log.info(
                                "[music-fetch] trackId={} jobId={} queue accepted",
                                trackId,
                                jobId);
                    } catch (RuntimeException schedulingError) {
                        heartbeat.dispose();
                        AppException unavailable = new AppException(
                                ErrorCode.MUSIC_FETCH_UNAVAILABLE,
                                "Spotify music fetch queue is full",
                                schedulingError);
                        return completeFailure(trackId, notifications)
                                .then(lock.release(trackId, token)
                                        .onErrorResume(error -> Mono.just(false)))
                                .then(Mono.<MusicFetchAcceptedResponse>error(unavailable))
                                .doFinally(signal -> waiters.retire(trackId, notifications));
                    }
                    return Mono.just(new MusicFetchAcceptedResponse(
                            trackId,
                            MusicFetchAcceptedResponse.Status.STARTED));
                })
                .doOnError(error -> {
                    if (registration != null) waiters.unregister(trackId, registration);
                });
    }

    private Mono<MusicFetchAcceptedResponse> completeAlreadyFetchedRequest(
            String trackId,
            FetchMusicWaiters.Registration registration,
            MusicTrack music) {
        MusicFetchAcceptedResponse response = new MusicFetchAcceptedResponse(
                trackId,
                MusicFetchAcceptedResponse.Status.ALREADY_FETCHED);
        return registration == null
                ? Mono.just(response)
                : completeSuccess(trackId, registration.state(), music).thenReturn(response);
    }

    private Mono<Void> executeJob(
            String trackId,
            String token,
            Path jobDirectory,
            FetchMusicWaiters.NotificationState notifications,
            LockHeartbeat heartbeat) {
        String jobId = jobDirectory.getFileName().toString();
        log.info(
                "[music-fetch] trackId={} jobId={} job started",
                trackId,
                jobId);
        Mono<MusicTrack> fetch = Mono.fromCallable(() -> Files.createDirectories(jobDirectory))
                .then(musicStore.findById(trackId)
                        .switchIfEmpty(Mono.error(new AppException(ErrorCode.MUSIC_NOT_FOUND))))
                .flatMap(music -> Boolean.TRUE.equals(music.getFetched())
                        ? Mono.just(music)
                        : resolveThumbnail(music)
                                .flatMap(enriched -> fetchRemoteMusic(
                                        enriched,
                                        trackId,
                                        jobDirectory,
                                        jobId)));
        Mono<MusicTrack> guardedFetch = heartbeat.isOwnershipLost()
                ? Mono.error(lockOwnershipLost())
                : Mono.firstWithSignal(
                        fetch,
                        heartbeat.ownershipLost().then(Mono.never()));

        Mono<Void> outcome = guardedFetch
                .flatMap(music -> completeSuccess(trackId, notifications, music))
                .onErrorResume(error -> {
                    log.error(
                            "[music-fetch] trackId={} jobId={} failed reason={}",
                            trackId,
                            jobId,
                            error.getMessage());
                    return completeFailure(trackId, notifications);
                });

        return outcome
                .then(finalizeJob(trackId, token, jobDirectory, notifications))
                .onErrorResume(error -> {
                    log.warn(
                            "|FetchMusicUseCase|job|finalization failed|trackId={}|error={}",
                            trackId,
                            error.getMessage());
                    waiters.retire(trackId, notifications);
                    return Mono.empty();
                })
                .doFinally(signal -> heartbeat.dispose());
    }

    private Mono<MusicTrack> fetchRemoteMusic(
            MusicTrack music,
            String trackId,
            Path jobDirectory,
            String jobId) {
        return Mono.usingWhen(
                artifactGateway.create(trackId)
                        .doOnNext(artifact -> log.info(
                                "[music-fetch] trackId={} jobId={} artifact created",
                                trackId,
                                jobId)),
                artifact -> artifactGateway.download(artifact, jobDirectory)
                        .doOnNext(downloaded -> log.info(
                                "[music-fetch] trackId={} jobId={} artifact downloaded file={}",
                                trackId,
                                jobId,
                                downloaded.file().getFileName()))
                        .flatMap(downloaded -> audioStorage
                                .uploadMusic(downloaded.file(), trackId)
                                .doOnNext(upload -> log.info(
                                        "[music-fetch] trackId={} jobId={} upload completed",
                                        trackId,
                                        jobId))
                                .flatMap(upload -> persistFetchedMusic(
                                        music,
                                        downloaded.descriptor().metadata().toSpotifyMetadata(),
                                        upload)
                                        .doOnNext(saved -> log.info(
                                                "[music-fetch] trackId={} jobId={} persistence completed",
                                                trackId,
                                                jobId))
                                        .onErrorResume(error -> cleanupUploadedAsset(
                                                upload,
                                                error)))),
                artifactGateway::cleanup,
                (artifact, error) -> artifactGateway.cleanup(artifact),
                artifactGateway::cleanup);
    }

    private LockHeartbeat startLockHeartbeat(String trackId, String token) {
        Duration ttl = properties.getLockTtl();
        long ttlNanos = Math.max(1L, ttl.toNanos());
        Duration period = Duration.ofNanos(Math.max(1L, ttlNanos / 3L));
        AtomicLong confirmedUntilNanos = new AtomicLong(System.nanoTime() + ttlNanos);
        Sinks.Empty<Void> ownershipLost = Sinks.empty();
        AtomicBoolean ownershipLostFlag = new AtomicBoolean();
        Disposable disposable = Flux.interval(period, period)
                .onBackpressureDrop()
                .concatMap(ignored -> renewLockBeforeDeadline(
                        trackId,
                        token,
                        ttlNanos,
                        period,
                        confirmedUntilNanos))
                .doOnNext(owned -> {
                    if (!owned) {
                        log.warn(
                                "|FetchMusicUseCase|lock heartbeat lost ownership|trackId={}",
                                trackId);
                        ownershipLostFlag.set(true);
                        ownershipLost.tryEmitError(lockOwnershipLost());
                    }
                })
                .takeUntil(owned -> !owned)
                .subscribe();
        return new LockHeartbeat(disposable, ownershipLost.asMono(), ownershipLostFlag);
    }

    private Mono<Boolean> renewLockBeforeDeadline(
            String trackId,
            String token,
            long ttlNanos,
            Duration period,
            AtomicLong confirmedUntilNanos) {
        long remainingNanos = confirmedUntilNanos.get() - System.nanoTime();
        if (remainingNanos <= 0L) {
            return Mono.just(false);
        }
        Duration attemptTimeout = Duration.ofNanos(Math.max(
                1L,
                Math.min(period.toNanos(), remainingNanos)));
        return lock.extend(trackId, token)
                .timeout(attemptTimeout)
                .map(extended -> {
                    if (Boolean.TRUE.equals(extended)) {
                        confirmedUntilNanos.set(System.nanoTime() + ttlNanos);
                        return true;
                    }
                    return false;
                })
                .onErrorResume(error -> {
                    log.warn(
                            "|FetchMusicUseCase|lock heartbeat failed|trackId={}|error={}",
                            trackId,
                            error.getMessage());
                    return Mono.just(System.nanoTime() < confirmedUntilNanos.get());
                });
    }

    private AppException lockOwnershipLost() {
        return new AppException(
                ErrorCode.MUSIC_FETCH_FAILED,
                "Spotify music fetch lock ownership was lost");
    }

    private Mono<MusicTrack> resolveThumbnail(MusicTrack music) {
        if (!isBlank(music.getDisplayImages())) {
            return Mono.just(music);
        }
        return thumbnailProvider.fetchThumbnail(music.getId())
                .map(image -> {
                    music.setDisplayImages(image);
                    return music;
                })
                .onErrorResume(error -> {
                    log.warn(
                            "|FetchMusicUseCase|oembed|best effort failed|trackId={}|error={}",
                            music.getId(),
                            error.getMessage());
                    return Mono.just(music);
                })
                .defaultIfEmpty(music);
    }

    private Mono<MusicTrack> persistFetchedMusic(
            MusicTrack music,
            SpotifyMusicMetadata metadata,
            MusicAudioUpload upload) {
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

        return musicStore.saveFetched(music, upload);
    }

    private Mono<MusicTrack> cleanupUploadedAsset(
            MusicAudioUpload upload,
            Throwable originalError) {
        return assetCleanup.deleteAsset(upload.publicId())
                .then(Mono.error(originalError));
    }

    private Mono<Void> emitImmediateSuccess(String userId, MusicTrack music) {
        return serialize(music)
                .flatMap(payload -> ssePublisher.sendToUser(
                        userId,
                        SUCCESS_EVENT,
                        payload));
    }

    private Mono<Void> completeSuccess(
            String trackId,
            FetchMusicWaiters.NotificationState notifications,
            MusicTrack music) {
        return serialize(music)
                .flatMap(payload -> completeAndNotify(
                        trackId,
                        notifications,
                        new FetchMusicWaiters.Terminal(SUCCESS_EVENT, payload)));
    }

    private Mono<Void> completeFailure(
            String trackId,
            FetchMusicWaiters.NotificationState notifications) {
        return serialize(new MusicFetchFailedEvent(trackId, SAFE_FAILURE_MESSAGE))
                .onErrorResume(error -> {
                    log.error(
                            "|FetchMusicUseCase|serialize failure event|trackId={}|error={}",
                            trackId,
                            error.getMessage());
                    return Mono.just(
                            "{\"trackId\":\"" + trackId
                                    + "\",\"message\":\""
                                    + SAFE_FAILURE_MESSAGE
                                    + "\"}");
                })
                .flatMap(payload -> completeAndNotify(
                        trackId,
                        notifications,
                        new FetchMusicWaiters.Terminal(FAILED_EVENT, payload)));
    }

    private Mono<Void> completeAndNotify(
            String trackId,
            FetchMusicWaiters.NotificationState notifications,
            FetchMusicWaiters.Terminal terminal) {
        Set<String> waiters = notifications.complete(terminal);
        return Flux.fromIterable(waiters)
                .flatMap(userId -> sendTerminal(userId, terminal)
                        .onErrorResume(error -> {
                            log.warn(
                                    "|FetchMusicUseCase|sse failed|trackId={}|userId={}|event={}|error={}",
                                    trackId,
                                    userId,
                                    terminal.event(),
                                    error.getMessage());
                            return Mono.empty();
                        }))
                .then()
                .doOnSuccess(ignored -> log.info(
                        "[music-fetch] trackId={} sse dispatched event={} waiterCount={}",
                        trackId,
                        terminal.event(),
                        waiters.size()));
    }

    private Mono<Void> sendTerminal(String userId, FetchMusicWaiters.Terminal terminal) {
        return ssePublisher.sendToUser(userId, terminal.event(), terminal.payload());
    }

    private Mono<String> serialize(Object payload) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(payload));
    }
    private Mono<Void> finalizeJob(
            String trackId,
            String token,
            Path jobDirectory,
            FetchMusicWaiters.NotificationState notifications) {
        return Mono.defer(() -> {
            Mono<Void> tempCleanup = Mono.<Void>fromRunnable(
                            () -> deleteJobDirectory(jobDirectory))
                    .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic())
                    .doOnSuccess(ignored -> log.info(
                            "[music-fetch] trackId={} temp cleanup completed",
                            trackId))
                    .onErrorResume(error -> {
                        log.warn(
                                "|FetchMusicUseCase|temp cleanup failed|trackId={}|error={}",
                                trackId,
                                error.getMessage());
                        return Mono.empty();
                    });
            Mono<Void> lockRelease = lock.release(trackId, token)
                    .doOnNext(released -> {
                        if (released) {
                            log.info(
                                    "[music-fetch] trackId={} lock released",
                                    trackId);
                        } else {
                            log.warn(
                                    "|FetchMusicUseCase|lock token no longer owns key|trackId={}",
                                    trackId);
                        }
                    })
                    .onErrorResume(error -> {
                        log.warn(
                                "|FetchMusicUseCase|lock release failed|trackId={}|error={}",
                                trackId,
                                error.getMessage());
                        return Mono.empty();
                    })
                    .then();
            return tempCleanup
                    .then(lockRelease)
                    .doFinally(signal -> {
                        waiters.retire(trackId, notifications);
                        log.info(
                                "[music-fetch] trackId={} job finalized signal={}",
                                trackId,
                                signal);
                    });
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

    private record LockHeartbeat(
            Disposable disposable,
            Mono<Void> ownershipLost,
            AtomicBoolean ownershipLostFlag) {
        boolean isOwnershipLost() {
            return ownershipLostFlag.get();
        }

        void dispose() {
            disposable.dispose();
        }
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
