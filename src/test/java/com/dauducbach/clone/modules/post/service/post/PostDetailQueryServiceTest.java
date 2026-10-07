package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.query.PostContentQueryService;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostDetailQueryServiceTest {

    @Mock
    PostContentQueryService postContentQueryService;
    @Mock
    PostItemRepository postItemRepository;
    @Mock
    MediaCatalog mediaCatalog;
    @Mock
    MusicCatalog musicCatalog;
    @Mock
    UserIdentityQuery userIdentityQuery;
    @Mock
    MediaAssets mediaAssets;

    @InjectMocks
    PostDetailQueryService service;

    @Test
    void returnsOrderedItemsWithCaptionsMediaAndTransformedMusic() {
        PostDetails post = PostDetails.builder()
                .postId("post-1")
                .userId("user-1")
                .content("Shared caption")
                .hashtag("[\"travel\"]")
                .validateStatus("APPROVED")
                .musicId("shared-music")
                .musicStart(10L)
                .musicEnd(40L)
                .createdAt(Instant.parse("2026-07-26T00:00:00Z"))
                .updatedAt(Instant.parse("2026-07-26T00:00:00Z"))
                .build();
        PostItem second = PostItem.builder()
                .id("item-2")
                .postId("post-1")
                .orderNumber(2)
                .mediaId("media-2")
                .caption("Second caption")
                .build();
        PostItem first = PostItem.builder()
                .id("item-1")
                .postId("post-1")
                .orderNumber(1)
                .mediaId("media-1")
                .caption("First caption")
                .build();
        MediaAssetView firstMedia = media("media-1", "public-1", "image", "jpg",
                "https://media/first.jpg", 1200, 1500);
        MediaAssetView secondMedia = media("media-2", "public-2", "video", "mp4",
                "https://media/second.mp4", 1080, 1920);
        MusicTrackView sharedMusic = new MusicTrackView("shared-music", null, "Midnight Echo",
                null, null, "North Avenue", "https://music/shared.mp3", 220L,
                null, null, null, null, null);

        when(postContentQueryService.findById("post-1")).thenReturn(Mono.just(post));
        when(postItemRepository.findByPostIdOrderByOrderNumberAsc("post-1"))
                .thenReturn(Flux.just(second, first));
        when(mediaCatalog.findById("media-1")).thenReturn(Mono.just(firstMedia));
        when(mediaCatalog.findById("media-2")).thenReturn(Mono.just(secondMedia));
        when(musicCatalog.findById("shared-music")).thenReturn(Mono.just(sharedMusic));
        when(userIdentityQuery.resolveIdentity("user-1"))
                .thenReturn(Mono.just(new UserIdentity("user-1", "bach", "Bach", "")));
        when(mediaAssets.transformMusicUrl("https://music/shared.mp3", 10L, 40L))
                .thenReturn("https://music/shared-transformed.mp3");
        when(mediaAssets.transformDeliveryUrl(isNull(), eq(MediaDisplayType.POST)))
                .thenReturn(null);
        when(mediaAssets.transformDeliveryUrl("https://media/first.jpg", MediaDisplayType.POST))
                .thenReturn("https://media/first.jpg");
        when(mediaAssets.transformDeliveryUrl("https://media/second.mp4", MediaDisplayType.POST))
                .thenReturn("https://media/second.mp4");

        StepVerifier.create(service.getPostDetail("post-1"))
                .assertNext(response -> {
                    org.junit.jupiter.api.Assertions.assertEquals("post-1", response.postId());
                    org.junit.jupiter.api.Assertions.assertEquals(List.of("travel"), response.hashtags());
                    org.junit.jupiter.api.Assertions.assertEquals("https://music/shared-transformed.mp3", response.music().playbackUrl());
                    org.junit.jupiter.api.Assertions.assertEquals(List.of("item-1", "item-2"),
                            response.items().stream().map(item -> item.id()).toList());
                    org.junit.jupiter.api.Assertions.assertEquals("First caption", response.items().getFirst().caption());
                    org.junit.jupiter.api.Assertions.assertEquals("https://media/first.jpg",
                            response.items().getFirst().media().secureUrl());
                })
                .verifyComplete();
    }

    private MediaAssetView media(
            String id,
            String publicId,
            String resourceType,
            String format,
            String secureUrl,
            int width,
            int height) {
        return new MediaAssetView(id, publicId, width, height, format, resourceType, 0,
                null, secureUrl, null, null, null, null, null, null, null);
    }
}
