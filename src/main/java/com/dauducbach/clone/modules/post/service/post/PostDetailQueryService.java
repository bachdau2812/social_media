package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaPlaybackUrls;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;

import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import com.dauducbach.clone.modules.post.query.PostContentQueryService;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PostDetailQueryService {
    private final PostContentQueryService postContentQueryService;
    private final PostItemRepository postItemRepository;
    private final MediaCatalog mediaCatalog;
    private final MusicCatalog musicCatalog;
    private final UserIdentityQuery userIdentityQuery;
    private final MediaPlaybackUrls mediaAssets;

    public Mono<PostItemResponse> getFirstItem(PostDetails post, MediaDisplayType mediaType) {
        boolean sharedMusic = hasText(post.getMusicId());
        return postItemRepository.findByPostIdOrderByOrderNumberAsc(post.getPostId())
                .sort(Comparator.comparing(PostItem::getOrderNumber, Comparator.nullsLast(Integer::compareTo)))
                .next()
                .flatMap(item -> mediaCatalog.findById(item.getMediaId())
                        .flatMap(media -> {
                            Mono<PostMusicResponse> music = sharedMusic
                                    ? Mono.empty()
                                    : resolveMusic(item.getMusicId(), item.getMusicStart(), item.getMusicEnd());
                            return music.map(value -> toItemResponse(item, media, value, mediaType))
                                    .defaultIfEmpty(toItemResponse(item, media, null, mediaType));
                        }))
                .switchIfEmpty(mediaCatalog.findFirstByOwnerIdAndOwnerType(post.getPostId(), OwnerType.POST)
                        .map(media -> toLegacyItemResponse(media, mediaType)));
    }

    public Mono<PostDetailResponse> getPostDetail(String postId) {
        return getPostDetail(postId, MediaDisplayType.POST);
    }

    public Mono<PostDetailResponse> getPostDetail(String postId, MediaDisplayType mediaType) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.POST : mediaType;
        return postContentQueryService.findById(postId)
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
                        userIdentityQuery.resolveIdentity(post.getUserId())
                                .onErrorReturn(new UserIdentity(
                                        post.getUserId(), post.getUserId(), post.getUserId(), "")))
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
                .concatMap(item -> mediaCatalog.findById(item.getMediaId())
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
        return musicCatalog.findById(musicId.trim())
                .map(music -> toMusicResponse(music, start, end))
                .onErrorResume(error -> Mono.empty());
    }

    private PostMusicResponse toMusicResponse(MusicTrackView music, Long start, Long end) {
        String playbackUrl = music.songUrl();
        if (hasText(playbackUrl) && start != null && end != null && start >= 0 && end > start) {
            playbackUrl = mediaAssets.transformMusicUrl(playbackUrl, start, end);
        }
        return new PostMusicResponse(
                music.id(),
                music.displayName(),
                music.singleName(),
                music.displayImages(),
                playbackUrl,
                start,
                end,
                music.duration()
        );
    }

    private PostItemResponse toItemResponse(PostItem item, MediaAssetView media, PostMusicResponse music, MediaDisplayType mediaType) {
        return new PostItemResponse(
                item.getId(),
                item.getOrderNumber(),
                item.getCaption(),
                toMediaResponse(media, mediaType),
                music
        );
    }

    private PostItemResponse toLegacyItemResponse(MediaAssetView media, MediaDisplayType mediaType) {
        return new PostItemResponse(
                media.assetId(),
                1,
                null,
                toMediaResponse(media, mediaType),
                null
        );
    }

    private PostMediaResponse toMediaResponse(MediaAssetView media, MediaDisplayType mediaType) {
        return new PostMediaResponse(
                media.assetId(),
                media.publicId(),
                media.mediaFormat(),
                media.resourceType(),
                mediaAssets.transformDeliveryUrl(media.url(), mediaType),
                mediaAssets.transformDeliveryUrl(media.secureUrl(), mediaType),
                media.displayName(),
                media.width(),
                media.height()
        );
    }

    private PostDetailResponse toResponse(PostDetails post, List<PostItemResponse> items, PostMusicResponse music, UserIdentity author) {
        return new PostDetailResponse(
                post.getPostId(),
                post.getUserId(),
                firstNonBlank(author.username(), post.getUserId()),
                firstNonBlank(author.fullName(), author.username(), post.getUserId()),
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
