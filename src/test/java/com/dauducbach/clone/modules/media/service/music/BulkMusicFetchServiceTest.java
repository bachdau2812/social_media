package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.media.constant.MusicFetchType;
import com.dauducbach.clone.modules.media.dto.music.request.BulkMusicFetchRequest;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchItemResponse;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BulkMusicFetchServiceTest {
    private static final String TRACK_1 = "1Gqm6KaobG2A1mFVjGnJsS";
    private static final String TRACK_2 = "2plbrEY59IikOBgBGLjaoe";
    private static final String TRACK_3 = "3n3Ppam7vgaVa1iaRUc9Lp";

    @Mock MusicsRepository repository;
    @Mock SpotifyMusicFetchService spotifyMusicFetchService;

    @Test
    void rejectsInvalidBatchRequestsBeforeSelection() {
        BulkMusicFetchService service = service();

        assertInvalid(service, new BulkMusicFetchRequest(null, List.of(), null));
        assertInvalid(service, new BulkMusicFetchRequest(MusicFetchType.ARTIST, List.of(" "), 20));
        assertInvalid(service, new BulkMusicFetchRequest(MusicFetchType.SONG, List.of("invalid"), null));
        assertInvalid(service, new BulkMusicFetchRequest(MusicFetchType.TOP, List.of(), 0));
        assertInvalid(service, new BulkMusicFetchRequest(MusicFetchType.TOP, List.of(), 101));
    }

    @Test
    void topUsesDefaultLimitAndMapsAcceptedStatus() {
        when(repository.findTopUnfetched(org.mockito.ArgumentMatchers.any(Pageable.class)))
                .thenReturn(Flux.just(music(TRACK_1)));
        when(spotifyMusicFetchService.requestFetchSilently(TRACK_1))
                .thenReturn(Mono.just(accepted(TRACK_1, MusicFetchAcceptedResponse.Status.STARTED)));

        StepVerifier.create(service().triggerFetch(
                        new BulkMusicFetchRequest(MusicFetchType.TOP, List.of("ignored"), null)))
                .assertNext(response -> {
                    assertThat(response.type()).isEqualTo(MusicFetchType.TOP);
                    assertThat(response.selectedCount()).isEqualTo(1);
                    assertThat(response.startedCount()).isEqualTo(1);
                })
                .verifyComplete();

        ArgumentCaptor<Pageable> pageable = ArgumentCaptor.forClass(Pageable.class);
        verify(repository).findTopUnfetched(pageable.capture());
        assertThat(pageable.getValue().getPageSize()).isEqualTo(20);
    }

    @Test
    void artistAppliesLimitPerArtistAndDeduplicatesTracksInStableOrder() {
        when(repository.findUnfetchedByArtist(
                org.mockito.ArgumentMatchers.eq("Artist One"),
                org.mockito.ArgumentMatchers.any(Pageable.class)))
                .thenReturn(Flux.just(music(TRACK_1), music(TRACK_2)));
        when(repository.findUnfetchedByArtist(
                org.mockito.ArgumentMatchers.eq("Artist Two"),
                org.mockito.ArgumentMatchers.any(Pageable.class)))
                .thenReturn(Flux.just(music(TRACK_2), music(TRACK_3)));
        when(spotifyMusicFetchService.requestFetchSilently(org.mockito.ArgumentMatchers.anyString()))
                .thenAnswer(invocation -> Mono.just(accepted(
                        invocation.getArgument(0), MusicFetchAcceptedResponse.Status.STARTED)));

        StepVerifier.create(service().triggerFetch(new BulkMusicFetchRequest(
                        MusicFetchType.ARTIST,
                        List.of(" Artist One ", "Artist One", "Artist Two"),
                        2)))
                .assertNext(response -> assertThat(response.items())
                        .extracting(item -> item.trackId())
                        .containsExactly(TRACK_1, TRACK_2, TRACK_3))
                .verifyComplete();

        InOrder order = inOrder(spotifyMusicFetchService);
        order.verify(spotifyMusicFetchService).requestFetchSilently(TRACK_1);
        order.verify(spotifyMusicFetchService).requestFetchSilently(TRACK_2);
        order.verify(spotifyMusicFetchService).requestFetchSilently(TRACK_3);
    }

    @Test
    void songKeepsOrderAndConvertsOneFailureWithoutStoppingBatch() {
        when(spotifyMusicFetchService.requestFetchSilently(TRACK_1))
                .thenReturn(Mono.just(accepted(TRACK_1, MusicFetchAcceptedResponse.Status.STARTED)));
        when(spotifyMusicFetchService.requestFetchSilently(TRACK_2))
                .thenReturn(Mono.error(new IllegalStateException("raw secret")));
        when(spotifyMusicFetchService.requestFetchSilently(TRACK_3))
                .thenReturn(Mono.just(accepted(TRACK_3, MusicFetchAcceptedResponse.Status.PROCESSING)));

        StepVerifier.create(service().triggerFetch(new BulkMusicFetchRequest(
                        MusicFetchType.SONG,
                        List.of(TRACK_1, TRACK_2, TRACK_1, TRACK_3),
                        100)))
                .assertNext(response -> {
                    assertThat(response.items()).extracting(item -> item.trackId())
                            .containsExactly(TRACK_1, TRACK_2, TRACK_3);
                    assertThat(response.failedCount()).isEqualTo(1);
                    assertThat(response.processingCount()).isEqualTo(1);
                    BulkMusicFetchItemResponse failed = response.items().get(1);
                    assertThat(failed.status()).isEqualTo(BulkMusicFetchItemResponse.Status.FAILED);
                    assertThat(failed.message()).isEqualTo("Music fetch failed");
                    assertThat(failed.message()).doesNotContain("raw secret");
                })
                .verifyComplete();
    }

    private BulkMusicFetchService service() {
        return new BulkMusicFetchService(repository, spotifyMusicFetchService);
    }

    private void assertInvalid(BulkMusicFetchService service, BulkMusicFetchRequest request) {
        StepVerifier.create(service.triggerFetch(request))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.MUSIC_REQUEST_INVALID)
                .verify();
    }

    private Musics music(String trackId) {
        return Musics.builder().id(trackId).fetched(false).build();
    }

    private MusicFetchAcceptedResponse accepted(
            String trackId,
            MusicFetchAcceptedResponse.Status status) {
        return new MusicFetchAcceptedResponse(trackId, status);
    }
}
