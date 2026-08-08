package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repositoty.music.MusicsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class MusicService {
    private static final Logger log = LoggerFactory.getLogger(MusicService.class);
    private static final int MAX_PAGE_SIZE = 100;

    MusicsRepository musicsRepository;

    public Mono<PageResponse<Musics>> getMusics(int page, int size, String keyword, String category) {
        int pageNumber = Math.max(page, 0);
        int pageSize = Math.clamp(size, 1, MAX_PAGE_SIZE);
        Pageable pageable = PageRequest.of(pageNumber, pageSize);
        String normalizedKeyword = normalizeOptional(keyword);
        String normalizedCategory = normalizeOptional(category);

        Mono<Long> countMono;
        Flux<Musics> musicFlux;
        if (normalizedKeyword != null && normalizedCategory != null) {
            countMono = musicsRepository.countSearchByCategory(normalizedKeyword, normalizedCategory);
            musicFlux = musicsRepository.searchByCategory(normalizedKeyword, normalizedCategory, pageable);
        } else if (normalizedKeyword != null) {
            countMono = musicsRepository.countSearch(normalizedKeyword);
            musicFlux = musicsRepository.search(normalizedKeyword, pageable);
        } else if (normalizedCategory != null) {
            countMono = musicsRepository.countByCategory(normalizedCategory);
            musicFlux = musicsRepository.findByCategory(normalizedCategory, pageable);
        } else {
            countMono = musicsRepository.count();
            musicFlux = musicsRepository.findPage(pageable);
        }

        log.info("|MusicService|getMusics|page={}|size={}|hasKeyword={}|category={}",
                pageNumber, pageSize, normalizedKeyword != null, normalizedCategory);
        return countMono.flatMap(total -> musicFlux.collectList()
                        .doOnSuccess(items -> log.info(
                                "|MusicService|getMusics|dbResult|page={}|count={}|total={}",
                                pageNumber, items.size(), total))
                        .map(items -> PageResponse.of(items, pageNumber, total, pageSize)))
                .doOnError(error -> log.error(
                        "|MusicService|getMusics|failed|page={}|size={}|error={}",
                        pageNumber, pageSize, error.getMessage()))
                .onErrorMap(error -> new AppException(
                        ErrorCode.MUSIC_FETCH_FAILED,
                        "Fetch musics failed",
                        error));
    }

    public Mono<Musics> getMusicById(String musicId) {
        if (musicId == null || musicId.isBlank()) {
            return Mono.error(new AppException(ErrorCode.MUSIC_REQUEST_INVALID, "musicId is required"));
        }

        String normalizedMusicId = musicId.trim();
        log.info("|MusicService|getMusicById|musicId={}", normalizedMusicId);
        return musicsRepository.findById(normalizedMusicId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.MUSIC_NOT_FOUND,
                        String.format("Music not found for musicId=%s", normalizedMusicId)
                )))
                .doOnSuccess(music -> log.info(
                        "|MusicService|getMusicById|success|musicId={}|slugName={}",
                        music.getId(), music.getSlugName()))
                .doOnError(error -> log.error(
                        "|MusicService|getMusicById|failed|musicId={}|error={}",
                        normalizedMusicId, error.getMessage()))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.MUSIC_FETCH_FAILED, "Fetch music detail failed", error));
    }

    private String normalizeOptional(String value) {
        return value == null || value.isBlank() ? null : value.trim();
    }
}
