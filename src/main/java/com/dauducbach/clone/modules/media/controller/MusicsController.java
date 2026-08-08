package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.dto.music.response.MusicFetchAcceptedResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.service.music.MusicService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/musics")
public class MusicsController {
    private final MusicService musicService;

    @PostMapping("/{trackId}/fetch")
    public Mono<ResponseEntity<ApiResponse<MusicFetchAcceptedResponse>>> fetchSpotifyMusic(
            @PathVariable String trackId,
            Authentication authentication) {
        if (authentication == null
                || !authentication.isAuthenticated()
                || authentication.getName() == null
                || authentication.getName().isBlank()) {
            return Mono.error(new AppException(
                    ErrorCode.AUTHENTICATION_FAILED,
                    "Authenticated user is required"));
        }
        return musicService.fetchSpotifyMusic(trackId, authentication.getName().trim())
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
