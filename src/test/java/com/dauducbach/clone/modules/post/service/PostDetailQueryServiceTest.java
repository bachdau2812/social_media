package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;

import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.repositoty.PostItemRepository;
import com.dauducbach.clone.modules.user.entity.Musics;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.MusicService;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
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
    PostService postService;
    @Mock
    PostItemRepository postItemRepository;
    @Mock
    MediaService mediaService;
    @Mock
    MusicService musicService;
    @Mock
    UserDetailsService userDetailsService;
    @Mock
    MediaCompatibilityFacade cloudinaryMediaService;

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
        Media firstMedia = Media.builder()
                .assetId("media-1")
                .publicId("public-1")
                .resourceType("image")
                .mediaFormat("jpg")
                .secureUrl("https://media/first.jpg")
                .width(1200)
                .height(1500)
                .build();
        Media secondMedia = Media.builder()
                .assetId("media-2")
                .publicId("public-2")
                .resourceType("video")
                .mediaFormat("mp4")
                .secureUrl("https://media/second.mp4")
                .width(1080)
                .height(1920)
                .build();
        Musics sharedMusic = Musics.builder()
                .id("shared-music")
                .displayName("Midnight Echo")
                .singleName("North Avenue")
                .songUrl("https://music/shared.mp3")
                .duration(220L)
                .build();

        when(postService.getPostById("post-1")).thenReturn(Mono.just(post));
        when(postItemRepository.findByPostIdOrderByOrderNumberAsc("post-1"))
                .thenReturn(Flux.just(second, first));
        when(mediaService.getById("media-1")).thenReturn(Mono.just(firstMedia));
        when(mediaService.getById("media-2")).thenReturn(Mono.just(secondMedia));
        when(musicService.getMusicById("shared-music")).thenReturn(Mono.just(sharedMusic));
        when(userDetailsService.getUserDetailsById("user-1"))
                .thenReturn(Mono.just(UserDetails.builder().userId("user-1").username("bach").fullName("Bach").build()));
        when(cloudinaryMediaService.transformMusicUrl("https://music/shared.mp3", 10L, 40L))
                .thenReturn("https://music/shared-transformed.mp3");
        when(cloudinaryMediaService.transformDeliveryUrl(isNull(), eq(MediaDisplayType.POST)))
                .thenReturn(null);
        when(cloudinaryMediaService.transformDeliveryUrl("https://media/first.jpg", MediaDisplayType.POST))
                .thenReturn("https://media/first.jpg");
        when(cloudinaryMediaService.transformDeliveryUrl("https://media/second.mp4", MediaDisplayType.POST))
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
}
