package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.dto.music.request.BulkMusicFetchRequest;
import com.dauducbach.clone.modules.media.dto.music.response.BulkMusicFetchResponse;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.service.music.BulkMusicFetchService;
import com.dauducbach.clone.modules.media.service.music.MusicService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/musics")
public class MusicsController {
    private static final Logger log = LoggerFactory.getLogger(MusicsController.class);
    private final MusicService musicService;
    private final BulkMusicFetchService bulkMusicFetchService;

    @PostMapping("/fetch")
    public Mono<ResponseEntity<ApiResponse<BulkMusicFetchResponse>>> triggerMusicFetch(
            @RequestBody BulkMusicFetchRequest request) {
        return bulkMusicFetchService.triggerFetch(request)
                .map(result -> ResponseEntity.accepted().body(
                        ApiResponse.<BulkMusicFetchResponse>builder()
                                .message("Bulk music fetch accepted")
                                .result(result)
                                .build()));
    }

    @PostMapping("/{trackId}/fetch")
    public Mono<ResponseEntity<ApiResponse<MusicFetchAcceptedResponse>>> fetchSpotifyMusic(
            @PathVariable String trackId,
            Authentication authentication) {
        if (authentication == null
                || !authentication.isAuthenticated()
                || authentication.getName() == null
                || authentication.getName().isBlank()) {
            log.warn(
                    "|MusicsController|fetchSpotifyMusic|rejected|trackId={}|reason=authentication_required",
                    trackId);
            return Mono.error(new AppException(
                    ErrorCode.AUTHENTICATION_FAILED,
                    "Authenticated user is required"));
        }
        log.info("|MusicsController|fetchSpotifyMusic|delegating|trackId={}", trackId);
        return musicService.fetchSpotifyMusic(trackId, authentication.getName().trim())
                .doOnNext(result -> log.info(
                        "|MusicsController|fetchSpotifyMusic|accepted|trackId={}|status={}",
                        trackId,
                        result.status()))
                .doOnError(error -> log.error(
                        "|MusicsController|fetchSpotifyMusic|failed|trackId={}|errorType={}",
                        trackId,
                        error.getClass().getSimpleName()))
                .map(result -> ResponseEntity.accepted().body(
                        ApiResponse.<MusicFetchAcceptedResponse>builder()
                                .message("Spotify music fetch accepted")
                                .result(result)
                                .build()));
    }

    @GetMapping
    public Mono<ApiResponse<PageResponse<Musics>>> getMusics(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String category
    ) {
        return musicService.getMusics(page, size, keyword, category)
                .map(response -> ApiResponse.<PageResponse<Musics>>builder()
                        .message("Musics fetched")
                        .result(response)
                        .build());
    }

    @GetMapping("/{musicId}")
    public Mono<ApiResponse<Musics>> getMusicById(@PathVariable String musicId) {
        return musicService.getMusicById(musicId)
                .map(response -> ApiResponse.<Musics>builder()
                        .message("Music fetched")
                        .result(response)
                        .build());
    }
}
