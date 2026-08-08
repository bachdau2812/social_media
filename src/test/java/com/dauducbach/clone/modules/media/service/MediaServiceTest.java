package com.dauducbach.clone.modules.media.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.dto.response.MediaAudioUploadResult;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.repository.MediaRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class MediaServiceTest {

    @Test
    void repeatedSaveForSamePublicIdAndOwnerReturnsExistingRegistryEntry() {
        MediaRepository repository = mock(MediaRepository.class);
        CloudinaryMediaService cloudinary = mock(CloudinaryMediaService.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        MediaService service = new MediaService(repository, cloudinary, template);
        Media existing = Media.builder()
                .assetId("asset-1")
                .publicId("public-1")
                .ownerId("post-1")
                .ownerType(OwnerType.POST)
                .build();

        when(repository.findByPublicId("public-1")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.saveCloudinaryMedia("public-1", "post-1", OwnerType.POST))
                .expectNext(existing)
                .verifyComplete();

        verifyNoInteractions(cloudinary, template);
    }

    @Test
    void repeatedPublicIdCannotBeAttachedToAnotherOwner() {
        MediaRepository repository = mock(MediaRepository.class);
        CloudinaryMediaService cloudinary = mock(CloudinaryMediaService.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        MediaService service = new MediaService(repository, cloudinary, template);
        Media existing = Media.builder()
                .assetId("asset-1")
                .publicId("public-1")
                .ownerId("post-1")
                .ownerType(OwnerType.POST)
                .build();

        when(repository.findByPublicId("public-1")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.saveCloudinaryMedia("public-1", "post-2", OwnerType.POST))
                .expectError(AppException.class)
                .verify();

        verifyNoInteractions(cloudinary, template);
    }

    @Test
    void registerFetchedMediaPersistsThroughMediaOwnerAndSetsDomainAttachment() {
        MediaRepository repository = mock(MediaRepository.class);
        CloudinaryMediaService cloudinary = mock(CloudinaryMediaService.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        @SuppressWarnings("unchecked")
        org.springframework.data.r2dbc.core.ReactiveInsertOperation.ReactiveInsert<Media> insert =
                mock(org.springframework.data.r2dbc.core.ReactiveInsertOperation.ReactiveInsert.class);
        MediaService service = new MediaService(repository, cloudinary, template);
        Media fetched = Media.builder()
                .assetId("asset-2")
                .publicId("public-2")
                .resourceType("image")
                .build();

        when(repository.findByPublicId("public-2")).thenReturn(Mono.empty());
        when(template.insert(Media.class)).thenReturn(insert);
        when(insert.using(org.mockito.ArgumentMatchers.any(Media.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.registerFetchedMedia(fetched, "comment-1", OwnerType.COMMENT))
                .assertNext(saved -> {
                    org.assertj.core.api.Assertions.assertThat(saved.getOwnerId()).isEqualTo("comment-1");
                    org.assertj.core.api.Assertions.assertThat(saved.getOwnerType()).isEqualTo(OwnerType.COMMENT);
                    org.assertj.core.api.Assertions.assertThat(saved.getUpdatedAt()).isNotNull();
                })
                .verifyComplete();

        verifyNoInteractions(cloudinary);
    }

    @Test
    void registerFetchedMediaRejectsExistingAssetOwnedByAnotherDomainObject() {
        MediaRepository repository = mock(MediaRepository.class);
        CloudinaryMediaService cloudinary = mock(CloudinaryMediaService.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        MediaService service = new MediaService(repository, cloudinary, template);
        Media existing = Media.builder()
                .assetId("asset-3")
                .publicId("public-3")
                .ownerId("message-1")
                .ownerType(OwnerType.CHAT_MESSAGE)
                .build();
        Media fetched = Media.builder()
                .assetId("asset-3")
                .publicId("public-3")
                .build();

        when(repository.findByPublicId("public-3")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.registerFetchedMedia(fetched, "message-2", OwnerType.CHAT_MESSAGE))
                .expectError(AppException.class)
                .verify();

        verifyNoInteractions(cloudinary, template);
    }

    @Test
    void saveFetchedMusicMediaIsIdempotentForTheSameMusicOwner() {
        MediaRepository repository = mock(MediaRepository.class);
        CloudinaryMediaService cloudinary = mock(CloudinaryMediaService.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        @SuppressWarnings("unchecked")
        org.springframework.data.r2dbc.core.ReactiveInsertOperation.ReactiveInsert<Media> insert =
                mock(org.springframework.data.r2dbc.core.ReactiveInsertOperation.ReactiveInsert.class);
        MediaService service = new MediaService(repository, cloudinary, template);
        MediaAudioUploadResult upload = new MediaAudioUploadResult(
                "asset-music", "1Gqm6KaobG2A1mFVjGnJsS", 0, 0,
                "flac", "video", 1234,
                "http://cdn/song.flac", "https://cdn/song.flac", "1", "version-1");
        Media existing = Media.builder()
                .assetId("asset-music")
                .publicId("1Gqm6KaobG2A1mFVjGnJsS")
                .ownerId("1Gqm6KaobG2A1mFVjGnJsS")
                .ownerType(OwnerType.MUSIC)
                .build();

        when(repository.findByPublicId("1Gqm6KaobG2A1mFVjGnJsS"))
                .thenReturn(Mono.empty(), Mono.just(existing));
        when(template.insert(Media.class)).thenReturn(insert);
        when(insert.using(org.mockito.ArgumentMatchers.any(Media.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(service.saveFetchedMusicMedia(
                        "1Gqm6KaobG2A1mFVjGnJsS", "Song", upload))
                .assertNext(saved -> {
                    org.assertj.core.api.Assertions.assertThat(saved.getOwnerId())
                            .isEqualTo("1Gqm6KaobG2A1mFVjGnJsS");
                    org.assertj.core.api.Assertions.assertThat(saved.getOwnerType())
                            .isEqualTo(OwnerType.MUSIC);
                })
                .verifyComplete();

        StepVerifier.create(service.saveFetchedMusicMedia(
                        "1Gqm6KaobG2A1mFVjGnJsS", "Song", upload))
                .expectNext(existing)
                .verifyComplete();

        org.mockito.Mockito.verify(insert, org.mockito.Mockito.times(1))
                .using(org.mockito.ArgumentMatchers.any(Media.class));
    }

    @Test
    void saveFetchedMusicMediaRejectsConflictingPublicIdOwnership() {
        MediaRepository repository = mock(MediaRepository.class);
        CloudinaryMediaService cloudinary = mock(CloudinaryMediaService.class);
        R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class);
        MediaService service = new MediaService(repository, cloudinary, template);
        MediaAudioUploadResult upload = new MediaAudioUploadResult(
                "asset-music", "stable-track-id", 0, 0,
                "flac", "video", 1234,
                "http://cdn/song.flac", "https://cdn/song.flac", "1", "version-1");
        Media existing = Media.builder()
                .assetId("asset-music")
                .publicId("stable-track-id")
                .ownerId("another-track")
                .ownerType(OwnerType.MUSIC)
                .build();
        when(repository.findByPublicId("stable-track-id")).thenReturn(Mono.just(existing));

        StepVerifier.create(service.saveFetchedMusicMedia("track-id", "Song", upload))
                .expectError(AppException.class)
                .verify();

        verifyNoInteractions(cloudinary, template);
    }
}
