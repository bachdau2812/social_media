package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;

import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repositoty.PostItemRepository;
import com.dauducbach.clone.modules.media.entity.music.Musics;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.media.service.music.MusicService;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PostDetailQueryService {
    private final PostService postService;
    private final PostItemRepository postItemRepository;
    private final MediaService mediaService;
    private final MusicService musicService;
    private final UserDetailsService userDetailsService;
    private final MediaCompatibilityFacade cloudinaryMediaService;

    public Mono<PostItemResponse> getFirstItem(PostDetails post, MediaDisplayType mediaType) {
        boolean sharedMusic = hasText(post.getMusicId());
        return postItemRepository.findByPostIdOrderByOrderNumberAsc(post.getPostId())
                .sort(Comparator.comparing(PostItem::getOrderNumber, Comparator.nullsLast(Integer::compareTo)))
                .next()
                .flatMap(item -> mediaService.getById(item.getMediaId())
                        .flatMap(media -> {
                            Mono<PostMusicResponse> music = sharedMusic
                                    ? Mono.empty()
                                    : resolveMusic(item.getMusicId(), item.getMusicStart(), item.getMusicEnd());
                            return music.map(value -> toItemResponse(item, media, value, mediaType))
                                    .defaultIfEmpty(toItemResponse(item, media, null, mediaType));
                        }))
                .switchIfEmpty(mediaService.getFirstByOwnerIdAndOwnerType(post.getPostId(), OwnerType.POST)
                        .map(media -> toLegacyItemResponse(media, mediaType)));
    }

    public Mono<PostDetailResponse> getPostDetail(String postId) {
        return getPostDetail(postId, MediaDisplayType.POST);
    }

    public Mono<PostDetailResponse> getPostDetail(String postId, MediaDisplayType mediaType) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.POST : mediaType;
        return postService.getPostById(postId)
                .flatMap(post -> buildPostDetail(post, displayType));
    }

    public Mono<PostDetailResponse> getRawPostDetail(PostDetails post) {
        return buildPostDetail(post, null);
    }

    public Mono<PostMusicResponse> getMusicResponse(String musicId, Long start, Long end) {
        return resolveMusic(musicId, start, end);
    }

    private Mono<PostDetailResponse> buildPostDetail(PostDetails post, MediaDisplayType mediaType) {
        return Mono.zip(
                        resolveItems(post, mediaType),
                        resolveMusic(post.getMusicId(), post.getMusicStart(), post.getMusicEnd())
                                .map(Optional::of)
                                .defaultIfEmpty(Optional.empty()),
                        userDetailsService.getUserDetailsById(post.getUserId())
                                .defaultIfEmpty(UserDetails.builder().userId(post.getUserId()).username(post.getUserId()).fullName(post.getUserId()).build()))
                .map(tuple -> toResponse(
                        post,
                        tuple.getT1(),
                        tuple.getT2().orElse(null),
                        tuple.getT3()
                ));
    }
    private Mono<List<PostItemResponse>> resolveItems(PostDetails post, MediaDisplayType mediaType) {
        boolean sharedMusic = hasText(post.getMusicId());
        return postItemRepository.findByPostIdOrderByOrderNumberAsc(post.getPostId())
                .sort(Comparator.comparing(PostItem::getOrderNumber, Comparator.nullsLast(Integer::compareTo)))
                .concatMap(item -> mediaService.getById(item.getMediaId())
                        .flatMap(media -> {
                            Mono<PostMusicResponse> music = sharedMusic
                                    ? Mono.empty()
                                    : resolveMusic(item.getMusicId(), item.getMusicStart(), item.getMusicEnd());
                            return music.map(value -> toItemResponse(item, media, value, mediaType))
                                    .defaultIfEmpty(toItemResponse(item, media, null, mediaType));
                        }))
                .collectList();
    }

    private Mono<PostMusicResponse> resolveMusic(String musicId, Long start, Long end) {
        if (!hasText(musicId)) {
            return Mono.empty();
        }
        return musicService.getMusicById(musicId.trim())
                .map(music -> toMusicResponse(music, start, end))
                .onErrorResume(error -> Mono.empty());
    }

    private PostMusicResponse toMusicResponse(Musics music, Long start, Long end) {
        String playbackUrl = music.getSongUrl();
        if (hasText(playbackUrl) && start != null && end != null && start >= 0 && end > start) {
            playbackUrl = cloudinaryMediaService.transformMusicUrl(playbackUrl, start, end);
        }
        return new PostMusicResponse(
                music.getId(),
                music.getDisplayName(),
                music.getSingleName(),
                music.getDisplayImages(),
                playbackUrl,
                start,
                end,
                music.getDuration()
        );
    }

    private PostItemResponse toItemResponse(PostItem item, Media media, PostMusicResponse music, MediaDisplayType mediaType) {
        return new PostItemResponse(
                item.getId(),
                item.getOrderNumber(),
                item.getCaption(),
                toMediaResponse(media, mediaType),
                music
        );
    }

    private PostItemResponse toLegacyItemResponse(Media media, MediaDisplayType mediaType) {
        return new PostItemResponse(
                media.getAssetId(),
                1,
                null,
                toMediaResponse(media, mediaType),
                null
        );
    }

    private PostMediaResponse toMediaResponse(Media media, MediaDisplayType mediaType) {
        return new PostMediaResponse(
                media.getAssetId(),
                media.getPublicId(),
                media.getMediaFormat(),
                media.getResourceType(),
                cloudinaryMediaService.transformDeliveryUrl(media.getUrl(), mediaType),
                cloudinaryMediaService.transformDeliveryUrl(media.getSecureUrl(), mediaType),
                media.getDisplayName(),
                media.getWidth(),
                media.getHeight()
        );
    }

    private PostDetailResponse toResponse(PostDetails post, List<PostItemResponse> items, PostMusicResponse music, UserDetails author) {
        return new PostDetailResponse(
                post.getPostId(),
                post.getUserId(),
                firstNonBlank(author.getUsername(), post.getUserId()),
                firstNonBlank(author.getFullName(), author.getUsername(), post.getUserId()),
                post.getContent(),
                post.getHashtag(),
                post.getHashtagList(),
                PostMediaRatio.defaultIfMissing(post.getMediaRatio()),
                post.getValidateStatus(),
                post.getMusicId(),
                post.getMusicStart(),
                post.getMusicEnd(),
                music,
                items,
                post.getCreatedAt(),
                post.getUpdatedAt()
        );
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) return value.trim();
        }
        return "";
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
