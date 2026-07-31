package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.feed.constant.FeedActivityType;
import com.dauducbach.clone.modules.feed.constant.FeedCacheKeys;
import com.dauducbach.clone.modules.feed.dto.cache.FeedPostDetailsCache;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedActorResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.FriendFeedActivityResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.CommentService;
import com.dauducbach.clone.modules.post.service.LikeService;
import com.dauducbach.clone.modules.post.service.PostDetailQueryService;
import com.dauducbach.clone.modules.post.service.PostFeedQueryService;
import com.dauducbach.clone.modules.post.service.RepostService;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.MediaForProfile;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import com.dauducbach.clone.utils.RedisUtil;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class FeedItemHydrator {
    private static final Logger log = LoggerFactory.getLogger(FeedItemHydrator.class);
    private static final int POST_DETAILS_CACHE_SCHEMA_VERSION = 6;
    private static final Duration POST_DETAILS_TTL = Duration.ofDays(1);
    private static final String FEED_RANKING_VERSION = "feed-v1";
    private static final String FEED_SOURCE_TYPE = "hybrid";
    private static final String FEED_RECOMMENDATION_REASON = "recommended_for_you";

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final PostFeedQueryService postFeedQueryService;
    private final PostDetailQueryService postDetailQueryService;
    private final MediaCompatibilityFacade mediaFacade;
    private final MediaService mediaService;
    private final MediaForProfile mediaForProfile;
    private final UserDetailsService userDetailsService;
    private final LikeService likeService;
    private final CommentService commentService;
    private final RepostService repostService;

    public Mono<FeedItemResponse> hydrate(String userId, String postId, MediaDisplayType mediaType) {
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;
        return postFeedQueryService.getApprovedPostById(postId)
                .flatMap(post -> getCachedPostDetails(postId)
                        .filter(cache -> Objects.equals(cache.getUpdatedAt(), post.getUpdatedAt()))
                        .switchIfEmpty(Mono.defer(() -> buildAndCachePostDetails(post))))
                .flatMap(cache -> Mono.zip(
                                likeService.countLikes(postId, EntityType.POST.name()).onErrorReturn(0L),
                                commentService.countCommentsByPostId(postId).onErrorReturn(0L),
                                repostService.countReposts(postId).onErrorReturn(0L),
                                likeService.hasLiked(userId, postId, EntityType.POST.name()).onErrorReturn(false),
                                repostService.hasReposted(userId, postId).onErrorReturn(false),
                                resolveFeedMusic(cache).map(Optional::of).defaultIfEmpty(Optional.empty())
                        )
                        .map(state -> toResponse(
                                userId,
                                cache,
                                displayType,
                                state.getT1(),
                                state.getT2(),
                                state.getT3(),
                                state.getT4(),
                                state.getT5(),
                                state.getT6().orElse(null)
                        )));
    }

    public Mono<FeedItemResponse> hydrateFriendActivity(
            String viewerUserId,
            FriendFeedActivityResponse activity,
            MediaDisplayType mediaType
    ) {
        if (activity == null || activity.postId() == null || activity.postId().isBlank()) {
            return Mono.empty();
        }

        FeedActivityType activityType = parseActivityType(activity.activityType());
        Mono<FeedItemResponse> hydratedPost = hydrate(viewerUserId, activity.postId(), mediaType);
        if (activityType != FeedActivityType.REPOST) {
            return hydratedPost.map(item -> item.withActivity(
                    activity.feedEntryId(), activityType, activity.activityAt(), null));
        }
        return hydratedPost.flatMap(item -> resolveFeedActor(activity.actorId())
                .map(actor -> item.withActivity(
                        activity.feedEntryId(), activityType, activity.activityAt(), actor)));
    }

    private Mono<FeedActorResponse> resolveFeedActor(String actorId) {
        String cleanActorId = actorId == null ? "" : actorId.trim();
        UserDetails fallback = UserDetails.builder()
                .userId(cleanActorId)
                .username(cleanActorId)
                .fullName(cleanActorId)
                .build();
        Mono<UserDetails> details = userDetailsService.getUserDetailsById(cleanActorId)
                .defaultIfEmpty(fallback)
                .onErrorReturn(fallback);
        Mono<String> avatar = mediaForProfile.getCurrentAvatar(cleanActorId, MediaDisplayType.AVATAR)
                .map(media -> firstNonBlank(media.getSecureUrl(), media.getUrl()))
                .defaultIfEmpty("")
                .onErrorReturn("");

        return Mono.zip(details, avatar)
                .map(result -> new FeedActorResponse(
                        cleanActorId,
                        firstNonBlank(result.getT1().getUsername(), cleanActorId),
                        firstNonBlank(
                                result.getT1().getFullName(),
                                result.getT1().getUsername(),
                                cleanActorId
                        ),
                        result.getT2()
                ));
    }

    private FeedActivityType parseActivityType(String value) {
        if (value == null || value.isBlank()) {
            return FeedActivityType.ORIGINAL_POST;
        }
        try {
            return FeedActivityType.valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ignored) {
            return FeedActivityType.ORIGINAL_POST;
        }
    }

    private Mono<FeedPostDetailsCache> getCachedPostDetails(String postId) {
        return redisTemplate.opsForValue()
                .get(FeedCacheKeys.postDetails(postId))
                .map(json -> RedisUtil.deserialize(json, FeedPostDetailsCache.class))
                .filter(cache -> cache != null
                        && cache.getSchemaVersion() == POST_DETAILS_CACHE_SCHEMA_VERSION
                        && cache.getPostId() != null
                        && !cache.getPostId().isBlank())
                .onErrorResume(error -> {
                    log.warn("|FeedItemHydrator|getCachedPostDetails|failed|postId={}|error={}",
                            postId, error.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<FeedPostDetailsCache> buildAndCachePostDetails(PostDetails post) {
        return buildPostDetailsCache(post)
                .flatMap(cache -> {
                    String json = RedisUtil.serialize(cache);
                    if (json == null) {
                        return Mono.just(cache);
                    }
                    return redisTemplate.opsForValue()
                            .set(FeedCacheKeys.postDetails(post.getPostId()), json, POST_DETAILS_TTL)
                            .onErrorResume(error -> {
                                log.warn("|FeedItemHydrator|buildAndCachePostDetails|cache write failed|postId={}|error={}",
                                        post.getPostId(), error.getMessage());
                                return Mono.empty();
                            })
                            .thenReturn(cache);
                });
    }

    private Mono<FeedPostDetailsCache> buildPostDetailsCache(PostDetails post) {
        UserDetails fallbackAuthor = UserDetails.builder()
                .userId(post.getUserId())
                .username(post.getUserId())
                .fullName(post.getUserId())
                .build();
        Mono<UserDetails> authorDetails = userDetailsService.getUserDetailsById(post.getUserId())
                .defaultIfEmpty(fallbackAuthor)
                .onErrorReturn(fallbackAuthor);
        Mono<String> authorAvatarUrl = mediaForProfile.getCurrentAvatar(post.getUserId(), MediaDisplayType.AVATAR)
                .map(avatar -> firstNonBlank(avatar.getSecureUrl(), avatar.getUrl()))
                .defaultIfEmpty("")
                .onErrorReturn("");
        Mono<List<FeedMediaResponse>> media = mediaService.getByOwnerId(post.getPostId(), OwnerType.POST)
                .map(this::toMediaResponse)
                .collectList()
                .onErrorReturn(List.of());
        Mono<PostDetailResponse> rawPostDetail = postDetailQueryService.getRawPostDetail(post);

        return Mono.zip(authorDetails, authorAvatarUrl, media, rawPostDetail)
                .map(tuple -> FeedPostDetailsCache.builder()
                        .schemaVersion(POST_DETAILS_CACHE_SCHEMA_VERSION)
                        .postId(post.getPostId())
                        .userId(post.getUserId())
                        .content(post.getContent())
                        .hashtag(post.getHashtag())
                        .hashtags(post.getHashtagList())
                        .mediaRatio(PostMediaRatio.defaultIfMissing(post.getMediaRatio()))
                        .createdAt(post.getCreatedAt())
                        .updatedAt(post.getUpdatedAt())
                        .validateStatus(post.getValidateStatus())
                        .authorUsername(firstNonBlank(tuple.getT1().getUsername(), post.getUserId()))
                        .authorFullName(firstNonBlank(
                                tuple.getT1().getFullName(),
                                tuple.getT1().getUsername(),
                                post.getUserId()))
                        .authorAvatarUrl(tuple.getT2())
                        .media(tuple.getT3())
                        .musicId(post.getMusicId())
                        .musicStart(post.getMusicStart())
                        .musicEnd(post.getMusicEnd())
                        .music(tuple.getT4().music())
                        .items(tuple.getT4().items())
                        .build());
    }

    private FeedItemResponse toResponse(
            String viewerUserId,
            FeedPostDetailsCache cache,
            MediaDisplayType mediaType,
            long likeCount,
            long commentCount,
            long repostCount,
            boolean likedByCurrentUser,
            boolean repostedByCurrentUser,
            PostMusicResponse music
    ) {
        List<FeedMediaResponse> media = cache.getMedia() == null
                ? List.of()
                : cache.getMedia().stream()
                .map(item -> transformMediaForDisplay(item, mediaType))
                .toList();
        List<PostItemResponse> items = cache.getItems() == null
                ? List.of()
                : cache.getItems().stream()
                .map(item -> transformItemForDisplay(item, mediaType))
                .toList();

        return new FeedItemResponse(
                cache.getPostId(),
                cache.getUserId(),
                cache.getAuthorUsername(),
                firstNonBlank(cache.getAuthorFullName(), cache.getAuthorUsername(), cache.getUserId()),
                firstNonBlank(cache.getAuthorAvatarUrl()),
                cache.getContent(),
                cache.getHashtags() == null ? List.of() : cache.getHashtags(),
                PostMediaRatio.defaultIfMissing(cache.getMediaRatio()),
                media,
                music,
                items,
                likeCount,
                commentCount,
                repostCount,
                likedByCurrentUser,
                repostedByCurrentUser,
                cache.getCreatedAt(),
                cache.getUpdatedAt(),
                FEED_SOURCE_TYPE,
                FEED_RECOMMENDATION_REASON,
                FEED_RANKING_VERSION,
                null,
                buildImpressionToken(viewerUserId, cache.getPostId()),
                null,
                null,
                null,
                null
        );
    }

    private Mono<PostMusicResponse> resolveFeedMusic(FeedPostDetailsCache cache) {
        String musicId = cache.getMusicId();
        if (musicId == null || musicId.isBlank()) {
            return Mono.empty();
        }
        return postDetailQueryService.getMusicResponse(musicId, cache.getMusicStart(), cache.getMusicEnd())
                .switchIfEmpty(Mono.justOrEmpty(cache.getMusic()))
                .onErrorResume(error -> Mono.justOrEmpty(cache.getMusic()));
    }

    private FeedMediaResponse transformMediaForDisplay(FeedMediaResponse media, MediaDisplayType mediaType) {
        return new FeedMediaResponse(
                media.assetId(),
                media.publicId(),
                media.mediaFormat(),
                media.resourceType(),
                mediaFacade.transformDeliveryUrl(media.url(), mediaType),
                mediaFacade.transformDeliveryUrl(media.secureUrl(), mediaType),
                media.displayName()
        );
    }

    private PostItemResponse transformItemForDisplay(PostItemResponse item, MediaDisplayType mediaType) {
        PostMediaResponse media = item.media();
        if (media == null) {
            return item;
        }
        return new PostItemResponse(
                item.id(),
                item.orderNumber(),
                item.caption(),
                new PostMediaResponse(
                        media.assetId(),
                        media.publicId(),
                        media.mediaFormat(),
                        media.resourceType(),
                        mediaFacade.transformDeliveryUrl(media.url(), mediaType),
                        mediaFacade.transformDeliveryUrl(media.secureUrl(), mediaType),
                        media.displayName(),
                        media.width(),
                        media.height()
                ),
                item.music()
        );
    }

    private FeedMediaResponse toMediaResponse(Media media) {
        return new FeedMediaResponse(
                media.getAssetId(),
                media.getPublicId(),
                media.getMediaFormat(),
                media.getResourceType(),
                media.getUrl(),
                media.getSecureUrl(),
                media.getDisplayName()
        );
    }

    private String buildImpressionToken(String viewerUserId, String postId) {
        String viewer = viewerUserId == null ? "" : viewerUserId.trim();
        String post = postId == null ? "" : postId.trim();
        String material = viewer + ":" + post + ":" + FEED_RANKING_VERSION;
        return FEED_RANKING_VERSION + ":" + UUID.nameUUIDFromBytes(material.getBytes(StandardCharsets.UTF_8));
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }
}
