package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class MusicCatalogService implements MusicCatalog {
    private final MusicsRepository musicsRepository;

    @Override
    public Flux<MusicTrackView> findAllByIds(Iterable<String> musicIds) {
        return musicsRepository.findAllById(musicIds).map(MusicCatalogService::toView);
    }

    @Override
    public Mono<MusicTrackView> findById(String musicId) {
        return musicsRepository.findById(musicId).map(MusicCatalogService::toView);
    }

    @Override
    public Mono<MusicTrackView> findBySlugNameAndDisplayName(String slugName, String displayName) {
        return musicsRepository.findBySlugNameAndDisplayName(slugName, displayName)
                .map(MusicCatalogService::toView);
    }

    @Override
    public Mono<MusicTrackView> findBySlugName(String slugName) {
        return musicsRepository.findBySlugName(slugName).map(MusicCatalogService::toView);
    }

    private static MusicTrackView toView(Musics music) {
        return new MusicTrackView(
                music.getId(),
                music.getSlugName(),
                music.getDisplayName(),
                music.getDescriptions(),
                music.getDisplayImages(),
                music.getSingleName(),
                music.getSongUrl(),
                music.getDuration(),
                music.getCategory(),
                music.getReleaseYear(),
                music.getAlbumName(),
                music.getFetched(),
                music.getPopularity());
    }
}
