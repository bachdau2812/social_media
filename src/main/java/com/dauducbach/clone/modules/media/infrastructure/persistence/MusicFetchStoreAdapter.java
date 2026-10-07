package com.dauducbach.clone.modules.media.infrastructure.persistence;

import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.media.music.fetch.MusicFetchStore;
import com.dauducbach.clone.modules.media.music.fetch.MusicAudioUpload;
import com.dauducbach.clone.modules.media.music.fetch.MusicTrack;
import com.dauducbach.clone.modules.media.repository.MusicsRepository;
import com.dauducbach.clone.modules.media.service.MediaService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
public class MusicFetchStoreAdapter implements MusicFetchStore {
    private final MusicsRepository musicsRepository;
    private final MediaService mediaService;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<MusicTrack> findById(String trackId) {
        return musicsRepository.findById(trackId).map(MusicFetchStoreAdapter::toTrack);
    }

    @Override
    public Mono<MusicTrack> saveFetched(MusicTrack track, MusicAudioUpload upload) {
        Mono<MusicTrack> databaseWork = mediaService.saveFetchedMusicMedia(
                        track.getId(),
                        track.getDisplayName(),
                        toMediaUpload(upload))
                .then(musicsRepository.save(toEntity(track)))
                .map(MusicFetchStoreAdapter::toTrack);
        return transactionalOperator.transactional(databaseWork);
    }

    private static MediaAudioUploadResult toMediaUpload(MusicAudioUpload upload) {
        return new MediaAudioUploadResult(
                upload.assetId(), upload.publicId(), upload.width(), upload.height(),
                upload.mediaFormat(), upload.resourceType(), upload.bytes(), upload.url(),
                upload.secureUrl(), upload.version(), upload.versionId());
    }

    private static MusicTrack toTrack(Musics music) {
        return MusicTrack.builder()
                .id(music.getId())
                .slugName(music.getSlugName())
                .displayName(music.getDisplayName())
                .descriptions(music.getDescriptions())
                .displayImages(music.getDisplayImages())
                .singleName(music.getSingleName())
                .songUrl(music.getSongUrl())
                .duration(music.getDuration())
                .category(music.getCategory())
                .releaseYear(music.getReleaseYear())
                .albumName(music.getAlbumName())
                .fetched(music.getFetched())
                .popularity(music.getPopularity())
                .build();
    }

    private static Musics toEntity(MusicTrack track) {
        return Musics.builder()
                .id(track.getId())
                .slugName(track.getSlugName())
                .displayName(track.getDisplayName())
                .descriptions(track.getDescriptions())
                .displayImages(track.getDisplayImages())
                .singleName(track.getSingleName())
                .songUrl(track.getSongUrl())
                .duration(track.getDuration())
                .category(track.getCategory())
                .releaseYear(track.getReleaseYear())
                .albumName(track.getAlbumName())
                .fetched(track.getFetched())
                .popularity(track.getPopularity())
                .build();
    }
}
