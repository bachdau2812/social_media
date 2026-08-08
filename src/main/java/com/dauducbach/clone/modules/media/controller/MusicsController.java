package com.dauducbach.clone.modules.media.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.dto.music.request.JamendoMusicImportRequest;
import com.dauducbach.clone.modules.media.dto.music.request.MusicCreateRequest;
import com.dauducbach.clone.modules.media.dto.music.response.JamendoMusicImportResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.service.music.MusicService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/musics")
public class MusicsController {
    private final MusicService musicService;

    @PostMapping("/import/jamendo")
    public Mono<ApiResponse<JamendoMusicImportResponse>> importFromJamendo(
            @Valid @RequestBody JamendoMusicImportRequest request,
            @RequestParam(required = false) String category
    ) {
        return musicService.importFromJamendo(request, category)
                .map(response -> ApiResponse.<JamendoMusicImportResponse>builder()
                        .message("Jamendo musics imported")
                        .result(response)
                        .build());
    }

    @PostMapping
    public Mono<ApiResponse<Musics>> createMusic(@Valid @RequestBody MusicCreateRequest request) {
        return musicService.createMusic(request)
                .map(response -> ApiResponse.<Musics>builder()
                        .message("Music created")
                        .result(response)
                        .build());
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
