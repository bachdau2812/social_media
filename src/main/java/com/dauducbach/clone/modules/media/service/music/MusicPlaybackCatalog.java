package com.dauducbach.clone.modules.media.service.music;

import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class MusicPlaybackCatalog {
    private final MusicsRepository musicsRepository;

    public Flux<Musics> findAllByIds(Iterable<String> musicIds) {
        return musicsRepository.findAllById(musicIds);
    }
}
