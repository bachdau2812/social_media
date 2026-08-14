package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.realtime.UserSsePublisher;
import com.dauducbach.clone.modules.media.configuration.SpotifyMusicFetchProperties;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import com.dauducbach.clone.modules.media.service.CloudinaryAudioStorageService;
import com.dauducbach.clone.modules.media.service.MediaAssetCleanupService;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.testsupport.TestLogCapture;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SpotifyMusicFetchServiceTest {
    private static final String TRACK_ID = "1Gqm6KaobG2A1mFVjGnJsS";

    @Mock MusicsRepository repository;
    @Mock SpotifyMusicFetchLock lock;
    @Mock SpotifyOEmbedClient oEmbed;
    @Mock SpotiFlacCliClient spotiFlac;
    @Mock FfprobeMetadataReader metadataReader;
    @Mock CloudinaryAudioStorageService audioStorage;
    @Mock MediaService mediaService;
    @Mock MediaAssetCleanupService cleanupService;
    @Mock UserSsePublisher ssePublisher;
    @Mock TransactionalOperator transactionalOperator;

    @TempDir
    Path tempDirectory;

    private Scheduler scheduler;
    private Scheduler processScheduler;
    private Queue<Runnable> scheduledTasks;
    private SpotifyMusicFetchProperties properties;

    @BeforeEach
    void setUp() {
        scheduledTasks = new ArrayDeque<>();
        scheduler = Schedulers.fromExecutor(scheduledTasks::add);
        processScheduler = Schedulers.immediate();
        properties = new SpotifyMusicFetchProperties();
        properties.setTempRoot(tempDirectory.toString());
    }

    @AfterEach
    void tearDown() {
        scheduler.dispose();
    }

    @Test
    void alreadyFetchedEmitsImmediateSuccessAndSkipsLock() {
        Musics music = catalogMusic();
        music.setFetched(true);
        music.setSongUrl("https://cdn/song.mp3");
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(ssePublisher.sendToUser(eq("user-1"), eq("music_fetch_success"), anyString()))
                .thenReturn(Mono.empty());

        StepVerifier.create(service().requestFetch(TRACK_ID, "user-1"))
                .assertNext(response -> assertThat(response.status())
                        .isEqualTo(MusicFetchAcceptedResponse.Status.ALREADY_FETCHED))
                .verifyComplete();

        verify(lock, never()).tryAcquire(anyString(), anyString());
        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_success"), anyString());
    }

    @Test
    void firstRequesterStartsExactlyOneJobAndSecondRequesterJoinsWaiters() throws Exception {
        Musics music = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("source.flac"), "audio");
        stubSuccessfulJob(music, flac);
        when(lock.tryAcquire(eq(TRACK_ID), anyString()))
                .thenReturn(Mono.just(true), Mono.just(false));

        SpotifyMusicFetchService service = service();
        StepVerifier.create(service.requestFetch(TRACK_ID, "user-1"))
                .assertNext(response -> assertThat(response.status())
                        .isEqualTo(MusicFetchAcceptedResponse.Status.STARTED))
                .verifyComplete();
        StepVerifier.create(service.requestFetch(TRACK_ID, "user-2"))
                .assertNext(response -> assertThat(response.status())
                        .isEqualTo(MusicFetchAcceptedResponse.Status.PROCESSING))
                .verifyComplete();

        runScheduledJobs();

        verify(spotiFlac, times(1)).download(eq(TRACK_ID), any(Path.class));
        ArgumentCaptor<Musics> savedMusic = ArgumentCaptor.forClass(Musics.class);
        verify(repository).save(savedMusic.capture());
        assertThat(savedMusic.getValue().getSongUrl()).isEqualTo("https://cdn/song.mp3");
        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_success"), anyString());
        verify(ssePublisher).sendToUser(
                eq("user-2"), eq("music_fetch_success"), anyString());
    }

    @Test
    void successfulJobLogsEveryExternalStageAndFinalization() throws Exception {
        Musics music = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("source.flac"), "audio");
        stubSuccessfulJob(music, flac);
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        try (TestLogCapture capture = TestLogCapture.start(SpotifyMusicFetchService.class)) {
            service().requestFetch(TRACK_ID, "user-1").block();
            runScheduledJobs();

            assertThat(capture.messages()).anyMatch(message -> message.contains("lock acquired"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("queue accepted"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("job started")
                    && message.contains("outputDirectory="));
            assertThat(capture.messages()).anyMatch(message -> message.contains("download completed"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("ffprobe completed"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("upload completed"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("persistence completed"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("sse dispatched")
                    && message.contains("music_fetch_success"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("temp cleanup completed"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("lock released"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("job finalized"));
        }
    }

    @Test
    void oEmbedFailureStillDownloadsAndPersists() throws Exception {
        Musics music = catalogMusic();
        music.setDisplayImages(null);
        Path flac = Files.writeString(tempDirectory.resolve("source.flac"), "audio");
        stubSuccessfulJob(music, flac);
        when(oEmbed.fetchThumbnail(TRACK_ID))
                .thenReturn(Mono.error(new IllegalStateException("oembed down")));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        service().requestFetch(TRACK_ID, "user-1").block();
        runScheduledJobs();

        verify(spotiFlac).download(eq(TRACK_ID), any(Path.class));
        verify(repository).save(any(Musics.class));
    }

    @Test
    void failureNotifiesEveryWaiterWithSafePayload() throws Exception {
        Musics music = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("source.flac"), "audio");
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(lock.tryAcquire(eq(TRACK_ID), anyString()))
                .thenReturn(Mono.just(true), Mono.just(false));
        when(spotiFlac.download(eq(TRACK_ID), any(Path.class))).thenReturn(Mono.just(flac));
        when(metadataReader.read(flac)).thenReturn(Mono.error(new IllegalStateException("raw secret")));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        SpotifyMusicFetchService service = service();
        service.requestFetch(TRACK_ID, "user-1").block();
        service.requestFetch(TRACK_ID, "user-2").block();
        runScheduledJobs();

        ArgumentCaptor<String> payload = ArgumentCaptor.forClass(String.class);
        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_failed"), payload.capture());
        verify(ssePublisher).sendToUser(
                eq("user-2"), eq("music_fetch_failed"), anyString());
        assertThat(payload.getValue()).contains(TRACK_ID)
                .contains("Kh\u00f4ng th\u1ec3 t\u1ea3i b\u00e0i h\u00e1t. Vui l\u00f2ng th\u1eed l\u1ea1i.")
                .doesNotContain("raw secret");
    }

    @Test
    void processFailureLogsReasonAndDispatchesFailureBeforeFinalization() {
        Musics music = catalogMusic();
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(spotiFlac.download(eq(TRACK_ID), any(Path.class)))
                .thenReturn(Mono.error(new java.util.concurrent.TimeoutException("process timeout")));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        try (TestLogCapture capture = TestLogCapture.start(SpotifyMusicFetchService.class)) {
            service().requestFetch(TRACK_ID, "user-1").block();
            runScheduledJobs();

            assertThat(capture.messages()).anyMatch(message -> message.contains("failed reason=process timeout")
                    && message.contains("jobId="));
            assertThat(capture.messages()).anyMatch(message -> message.contains("sse dispatched")
                    && message.contains("music_fetch_failed"));
            assertThat(capture.messages()).anyMatch(message -> message.contains("job finalized"));
        }

        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_failed"), anyString());
        verify(lock).release(eq(TRACK_ID), anyString());
    }

    @Test
    void databaseFailureDeletesUploadedCloudinaryAsset() throws Exception {
        Musics music = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("source.flac"), "audio");
        stubUntilPersistence(music, flac);
        when(repository.save(any(Musics.class)))
                .thenReturn(Mono.error(new IllegalStateException("database down")));
        when(cleanupService.delete("social_network_musics/" + TRACK_ID))
                .thenReturn(Mono.empty());
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        service().requestFetch(TRACK_ID, "user-1").block();
        runScheduledJobs();

        verify(cleanupService).delete("social_network_musics/" + TRACK_ID);
    }

    @Test
    void finalizationDeletesTempDirectoryAndReleasesOwnedLock() throws Exception {
        Musics music = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("source.flac"), "audio");
        stubSuccessfulJob(music, flac);
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        service().requestFetch(TRACK_ID, "user-1").block();
        runScheduledJobs();

        ArgumentCaptor<Path> jobDirectory = ArgumentCaptor.forClass(Path.class);
        verify(spotiFlac).download(eq(TRACK_ID), jobDirectory.capture());
        assertThat(jobDirectory.getValue().normalize().startsWith(tempDirectory.normalize())).isTrue();
        assertThat(jobDirectory.getValue()).doesNotExist();
        verify(lock, atLeastOnce()).release(eq(TRACK_ID), anyString());
    }

    @Test
    void queueRejectionIsReturnedSynchronouslyAndReleasesTheOwnedLock() {
        Musics music = catalogMusic();
        Scheduler rejectingScheduler = org.mockito.Mockito.mock(Scheduler.class);
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(rejectingScheduler.schedule(any(Runnable.class)))
                .thenThrow(new RejectedExecutionException("queue full"));

        StepVerifier.create(service(rejectingScheduler).requestFetch(TRACK_ID, "user-1"))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode())
                            .isEqualTo(ErrorCode.MUSIC_FETCH_UNAVAILABLE);
                })
                .verify();

        verify(lock).release(eq(TRACK_ID), anyString());
        verify(spotiFlac, never()).download(eq(TRACK_ID), any(Path.class));
    }

    @Test
    void queueRejectionNotifiesRequesterThatAlreadyJoinedProcessingState() {
        Musics music = catalogMusic();
        Scheduler rejectingScheduler = org.mockito.Mockito.mock(Scheduler.class);
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(lock.tryAcquire(eq(TRACK_ID), anyString()))
                .thenReturn(Mono.just(true), Mono.just(false));
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());

        AtomicReference<SpotifyMusicFetchService> serviceRef = new AtomicReference<>();
        when(rejectingScheduler.schedule(any(Runnable.class)))
                .thenAnswer(invocation -> {
                    MusicFetchAcceptedResponse joined = serviceRef.get()
                            .requestFetch(TRACK_ID, "user-2")
                            .block();
                    assertThat(joined.status())
                            .isEqualTo(MusicFetchAcceptedResponse.Status.PROCESSING);
                    throw new RejectedExecutionException("queue full");
                });

        SpotifyMusicFetchService service = service(rejectingScheduler);
        serviceRef.set(service);
        StepVerifier.create(service.requestFetch(TRACK_ID, "user-1"))
                .expectError(AppException.class)
                .verify();

        verify(ssePublisher).sendToUser(
                eq("user-2"), eq("music_fetch_failed"), anyString());
    }

    @Test
    void lateWaiterReceivesTerminalSuccessWithoutStartingAnotherJob() throws Exception {
        Musics requestView = catalogMusic();
        Musics jobView = catalogMusic();
        Musics lateStaleView = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("late-waiter.flac"), "audio");
        stubUntilPersistence(jobView, flac);
        when(repository.findById(TRACK_ID)).thenReturn(
                Mono.just(requestView),
                Mono.just(jobView),
                Mono.just(lateStaleView));
        when(repository.save(any(Musics.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));

        AtomicReference<SpotifyMusicFetchService> serviceRef = new AtomicReference<>();
        AtomicBoolean lateRequestSent = new AtomicBoolean();
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenAnswer(invocation -> {
                    if ("user-1".equals(invocation.getArgument(0))
                            && "music_fetch_success".equals(invocation.getArgument(1))
                            && lateRequestSent.compareAndSet(false, true)) {
                        serviceRef.get().requestFetch(TRACK_ID, "user-2").block();
                    }
                    return Mono.empty();
                });

        SpotifyMusicFetchService service = service();
        serviceRef.set(service);
        service.requestFetch(TRACK_ID, "user-1").block();
        runScheduledJobs();

        verify(ssePublisher).sendToUser(
                eq("user-2"), eq("music_fetch_success"), anyString());
        verify(lock, times(1)).tryAcquire(eq(TRACK_ID), anyString());
        verify(spotiFlac, times(1)).download(eq(TRACK_ID), any(Path.class));
    }

    @Test
    void acquiredJobRechecksFetchedStateBeforeDownloading() {
        Musics stale = catalogMusic();
        Musics fetched = catalogMusic();
        fetched.setFetched(true);
        fetched.setSongUrl("https://cdn/already-fetched.flac");
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(stale), Mono.just(fetched));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(ssePublisher.sendToUser(anyString(), eq("music_fetch_success"), anyString()))
                .thenReturn(Mono.empty());

        service().requestFetch(TRACK_ID, "user-1").block();
        runScheduledJobs();

        verify(spotiFlac, never()).download(eq(TRACK_ID), any(Path.class));
        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_success"), anyString());
    }

    @Test
    void failedConcurrentRequestDoesNotRemoveSameUsersActiveWaiter() throws Exception {
        Musics music = catalogMusic();
        Path flac = Files.writeString(tempDirectory.resolve("same-user.flac"), "audio");
        stubSuccessfulJob(music, flac);
        when(lock.tryAcquire(eq(TRACK_ID), anyString()))
                .thenReturn(
                        Mono.just(true),
                        Mono.error(new IllegalStateException("redis unavailable")));

        SpotifyMusicFetchService service = service();
        service.requestFetch(TRACK_ID, "user-1").block();
        StepVerifier.create(service.requestFetch(TRACK_ID, "user-1"))
                .expectErrorMessage("redis unavailable")
                .verify();

        runScheduledJobs();

        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_success"), anyString());
    }

    @Test
    void queuedJobRenewsLockAndDoesNotRunAfterOwnershipIsLost() throws Exception {
        Musics music = catalogMusic();
        properties.setLockTtl(Duration.ofMillis(30));
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(lock.extend(eq(TRACK_ID), anyString())).thenReturn(Mono.just(false));
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());

        SpotifyMusicFetchService service = service();
        service.requestFetch(TRACK_ID, "user-1").block();

        Thread.sleep(1_200);
        runScheduledJobs();

        verify(lock, atLeastOnce()).extend(eq(TRACK_ID), anyString());
        verify(spotiFlac, never()).download(eq(TRACK_ID), any(Path.class));
        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_failed"), anyString());
    }

    @Test
    void renewalThatHangsPastConfirmedLeaseDeadlineFencesQueuedJob() throws Exception {
        Musics music = catalogMusic();
        properties.setLockTtl(Duration.ofMillis(90));
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(lock.tryAcquire(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(lock.extend(eq(TRACK_ID), anyString())).thenReturn(Mono.never());
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());

        SpotifyMusicFetchService service = service();
        service.requestFetch(TRACK_ID, "user-1").block();

        Thread.sleep(180);
        runScheduledJobs();

        verify(lock, atLeastOnce()).extend(eq(TRACK_ID), anyString());
        verify(spotiFlac, never()).download(eq(TRACK_ID), any(Path.class));
        verify(ssePublisher).sendToUser(
                eq("user-1"), eq("music_fetch_failed"), anyString());
    }

    @Test
    void invalidTrackIdFailsBeforeRepositoryLookup() {
        StepVerifier.create(service().requestFetch("invalid", "user-1"))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode())
                            .isEqualTo(ErrorCode.MUSIC_REQUEST_INVALID);
                })
                .verify();

        verify(repository, never()).findById(anyString());
    }

    private void runScheduledJobs() {
        Runnable task;
        while ((task = scheduledTasks.poll()) != null) {
            task.run();
        }
    }

    private SpotifyMusicFetchService service() {
        return service(scheduler);
    }

    private SpotifyMusicFetchService service(Scheduler selectedJobScheduler) {
        return new SpotifyMusicFetchService(
                repository,
                lock,
                oEmbed,
                spotiFlac,
                metadataReader,
                audioStorage,
                mediaService,
                cleanupService,
                ssePublisher,
                new ObjectMapper(),
                transactionalOperator,
                properties,
                selectedJobScheduler,
                processScheduler);
    }

    private Musics catalogMusic() {
        return Musics.builder()
                .id(TRACK_ID)
                .slugName("song")
                .displayName("Song")
                .singleName("Artist")
                .albumName("Album")
                .releaseYear((short) 2024)
                .duration(186L)
                .displayImages("https://existing/cover")
                .fetched(false)
                .build();
    }

    private void stubSuccessfulJob(Musics music, Path flac) {
        stubUntilPersistence(music, flac);
        when(repository.save(any(Musics.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
    }

    private void stubUntilPersistence(Musics music, Path flac) {
        when(repository.findById(TRACK_ID)).thenReturn(Mono.just(music));
        when(spotiFlac.download(eq(TRACK_ID), any(Path.class))).thenReturn(Mono.just(flac));
        when(metadataReader.read(flac)).thenReturn(Mono.just(new SpotifyMusicMetadata(
                "Downloaded title",
                "Downloaded artist",
                "Downloaded album",
                "Album Artist",
                "Composer",
                "Rap/Hip Hop",
                "Lyrics")));
        when(audioStorage.uploadMusic(flac, TRACK_ID)).thenReturn(Mono.just(uploadResult()));
        when(mediaService.saveFetchedMusicMedia(eq(TRACK_ID), anyString(), any(MediaAudioUploadResult.class)))
                .thenReturn(Mono.just(Media.builder().assetId("asset-1").build()));
        when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        when(ssePublisher.sendToUser(anyString(), anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(lock.release(eq(TRACK_ID), anyString())).thenReturn(Mono.just(true));
    }

    private MediaAudioUploadResult uploadResult() {
        return new MediaAudioUploadResult(
                "asset-1",
                "social_network_musics/" + TRACK_ID,
                0,
                0,
                "flac",
                "video",
                1234,
                "http://cdn/song.mp3",
                "https://cdn/song.mp3",
                "1",
                "version-1");
    }
}
