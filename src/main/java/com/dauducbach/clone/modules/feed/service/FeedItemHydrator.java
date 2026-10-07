package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.constant.FeedActivityType;
import com.dauducbach.clone.modules.feed.dto.response.FeedActorResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
import com.dauducbach.clone.modules.feed.hydration.FeedHydrationBatch;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.response.FriendFeedActivityResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.publicapi.PostQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class FeedItemHydrator {
    private static final String FEED_RANKING_VERSION = "feed-v1";
    private static final String FEED_SOURCE_TYPE = "hybrid";
    private static final String FEED_RECOMMENDATION_REASON = "recommended_for_you";

    private final PostQuery postQuery;
    private final PostInteractionQuery postInteractionQuery;
    private final MediaCatalog mediaCatalog;
    private final MusicCatalog musicCatalog;
    private final MediaAssets mediaAssets;
    private final UserIdentityQuery userIdentityQuery;

    public Mono<List<FeedItemResponse>> hydratePage(
            String viewerId,
            List<String> candidatePostIds,
            MediaDisplayType mediaType) {
        List<String> ids = cleanIds(candidatePostIds);
        if (ids.isEmpty()) return Mono.just(List.of());
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;
        return loadBatch(viewerId, ids, List.of())
                .map(batch -> ids.stream()
                        .map(batch.postsById()::get)
                        .filter(java.util.Objects::nonNull)
                        .map(post -> toResponse(viewerId, post, batch, displayType))
                        .toList());
    }

    public Mono<FeedItemResponse> hydrate(String viewerId, String postId, MediaDisplayType mediaType) {
        if (!hasText(postId)) return Mono.empty();
        return hydratePage(viewerId, List.of(postId), mediaType)
                .flatMap(items -> items.isEmpty() ? Mono.empty() : Mono.just(items.get(0)));
    }

    public Mono<List<FeedItemResponse>> hydrateFriendActivities(
            String viewerId,
            List<FriendFeedActivityResponse> activities,
            MediaDisplayType mediaType) {
        List<FriendFeedActivityResponse> validActivities = activities == null ? List.of() : activities.stream()
                .filter(activity -> activity != null && hasText(activity.postId()))
                .toList();
        if (validActivities.isEmpty()) return Mono.just(List.of());
        List<String> postIds = cleanIds(validActivities.stream().map(FriendFeedActivityResponse::postId).toList());
        List<String> actorIds = validActivities.stream()
                .filter(activity -> parseActivityType(activity.activityType()) == FeedActivityType.REPOST)
                .map(FriendFeedActivityResponse::actorId)
                .toList();
        MediaDisplayType displayType = mediaType == null ? MediaDisplayType.FEED : mediaType;
        return loadBatch(viewerId, postIds, actorIds).map(batch -> validActivities.stream()
                .map(activity -> {
                    PostQuery.FeedPostSnapshot post = batch.postsById().get(activity.postId());
                    if (post == null) return null;
                    FeedItemResponse item = toResponse(viewerId, post, batch, displayType);
                    FeedActivityType activityType = parseActivityType(activity.activityType());
                    FeedActorResponse actor = activityType == FeedActivityType.REPOST
                            ? toActor(activity.actorId(), batch.identitiesByUserId())
                            : null;
                    return item.withActivity(activity.feedEntryId(), activityType, activity.activityAt(), actor);
                })
                .filter(java.util.Objects::nonNull)
                .toList());
    }

    public Mono<FeedItemResponse> hydrateFriendActivity(
            String viewerId,
            FriendFeedActivityResponse activity,
            MediaDisplayType mediaType) {
        return hydrateFriendActivities(viewerId, List.of(activity), mediaType)
                .flatMap(items -> items.isEmpty() ? Mono.empty() : Mono.just(items.get(0)));
    }

    private Mono<FeedHydrationBatch> loadBatch(String viewerId, List<String> postIds, List<String> additionalIdentityIds) {
        return postQuery.findApprovedFeedSnapshots(postIds).collectList().flatMap(posts -> {
            Map<String, PostQuery.FeedPostSnapshot> postsById = posts.stream()
                    .filter(post -> hasText(post.postId()))
                    .collect(Collectors.toMap(PostQuery.FeedPostSnapshot::postId, post -> post, (first, ignored) -> first));
            List<String> availablePostIds = cleanIds(posts.stream().map(PostQuery.FeedPostSnapshot::postId).toList());
            List<String> identityIds = new ArrayList<>(posts.stream()
                    .map(PostQuery.FeedPostSnapshot::ownerId).toList());
            if (additionalIdentityIds != null) identityIds.addAll(additionalIdentityIds);
            identityIds = cleanIds(identityIds);
            List<String> assetIds = cleanIds(posts.stream().flatMap(post -> post.items().stream())
                    .map(PostQuery.FeedItemSnapshot::mediaId).toList());
            List<String> musicIds = cleanIds(posts.stream().flatMap(post -> {
                List<String> ids = new ArrayList<>();
                if (hasText(post.musicId())) ids.add(post.musicId());
                else post.items().stream().map(PostQuery.FeedItemSnapshot::musicId).filter(this::hasText).forEach(ids::add);
                return ids.stream();
            }).toList());

            Mono<Map<String, PostInteractionQuery.Snapshot>> interactions = postInteractionQuery
                    .findSnapshots(postIds, viewerId)
                    .collectMap(PostInteractionQuery.SnapshotEntry::postId, PostInteractionQuery.SnapshotEntry::snapshot)
                    .onErrorReturn(Map.of());
            Mono<Map<String, UserIdentity>> identities = identityIds.isEmpty() ? Mono.just(Map.of())
                    : userIdentityQuery.findIdentities(identityIds).collectMap(UserIdentity::userId).onErrorReturn(Map.of());
            Mono<Map<String, MediaAssetView>> assets = assetIds.isEmpty() ? Mono.just(Map.of())
                    : mediaCatalog.findByIds(assetIds).collectMap(MediaAssetView::assetId).onErrorReturn(Map.of());
            Mono<Map<String, List<MediaAssetView>>> mediaByOwner = availablePostIds.isEmpty() ? Mono.just(Map.of())
                    : mediaCatalog.findByOwnerIds(availablePostIds, OwnerType.POST)
                    .collectMultimap(MediaAssetView::ownerId)
                    .map(grouped -> grouped.entrySet().stream().collect(Collectors.toMap(
                            Map.Entry::getKey, entry -> List.copyOf(entry.getValue()))))
                    .onErrorReturn(Map.of());
            Mono<Map<String, MusicTrackView>> music = musicIds.isEmpty() ? Mono.just(Map.of())
                    : musicCatalog.findAllByIds(musicIds).collectMap(MusicTrackView::id).onErrorReturn(Map.of());

            return Mono.zip(interactions, identities, assets, mediaByOwner, music)
                    .map(result -> new FeedHydrationBatch(postsById, result.getT1(), result.getT2(), result.getT3(),
                            result.getT4(), result.getT5()));
        });
    }

    private FeedItemResponse toResponse(
            String viewerId,
            PostQuery.FeedPostSnapshot post,
            FeedHydrationBatch batch,
            MediaDisplayType displayType) {
        UserIdentity author = identity(post.ownerId(), batch.identitiesByUserId());
        List<MediaAssetView> ownerAssets = batch.mediaByOwnerId().getOrDefault(post.postId(), List.of()).stream()
                .sorted(Comparator.comparing(MediaAssetView::createdAt, Comparator.nullsLast(Comparator.naturalOrder()))
                        .thenComparing(MediaAssetView::assetId, Comparator.nullsLast(Comparator.naturalOrder())))
                .toList();
        List<FeedMediaResponse> media = ownerAssets.stream().map(asset -> toFeedMedia(asset, displayType)).toList();
        List<PostItemResponse> items = post.items().stream()
                .sorted(Comparator.comparing(PostQuery.FeedItemSnapshot::orderNumber,
                        Comparator.nullsLast(Comparator.naturalOrder())))
                .map(item -> toPostItem(post, item, batch, displayType))
                .filter(java.util.Objects::nonNull)
                .toList();
        if (items.isEmpty() && post.items().isEmpty() && !ownerAssets.isEmpty()) {
            MediaAssetView first = ownerAssets.get(0);
            items = List.of(new PostItemResponse(first.assetId(), 1, null, toPostMedia(first, displayType), null));
        }
        PostInteractionQuery.Snapshot interaction = batch.interactionsByPostId().getOrDefault(post.postId(),
                new PostInteractionQuery.Snapshot(0, 0, 0, false, false));
        PostMusicResponse sharedMusic = resolveMusic(hasText(post.musicId())
                ? batch.musicById().get(post.musicId()) : null, post.musicStart(), post.musicEnd());
        return new FeedItemResponse(
                post.postId(), post.ownerId(), author.username(), firstNonBlank(author.fullName(), author.username(), post.ownerId()),
                author.avatarUrl(), post.content(), post.hashtags(), PostMediaRatio.defaultIfMissing(post.mediaRatio()),
                media, sharedMusic, items, interaction.likes(), interaction.comments(), interaction.reposts(),
                interaction.likedByViewer(), interaction.repostedByViewer(), post.createdAt(), post.updatedAt(),
                FEED_SOURCE_TYPE, FEED_RECOMMENDATION_REASON, FEED_RANKING_VERSION, null,
                buildImpressionToken(viewerId, post.postId()), null, null, null, null);
    }

    private PostItemResponse toPostItem(
            PostQuery.FeedPostSnapshot post,
            PostQuery.FeedItemSnapshot item,
            FeedHydrationBatch batch,
            MediaDisplayType displayType) {
        MediaAssetView asset = hasText(item.mediaId()) ? batch.assetsById().get(item.mediaId()) : null;
        if (asset == null) return null;
        PostMusicResponse itemMusic = hasText(post.musicId()) ? null
                : resolveMusic(hasText(item.musicId()) ? batch.musicById().get(item.musicId()) : null,
                        item.musicStart(), item.musicEnd());
        return new PostItemResponse(item.id(), item.orderNumber(), item.caption(), toPostMedia(asset, displayType), itemMusic);
    }

    private FeedMediaResponse toFeedMedia(MediaAssetView asset, MediaDisplayType displayType) {
        return new FeedMediaResponse(asset.assetId(), asset.publicId(), asset.mediaFormat(), asset.resourceType(),
                mediaAssets.transformDeliveryUrl(asset.url(), displayType),
                mediaAssets.transformDeliveryUrl(asset.secureUrl(), displayType), asset.displayName());
    }

    private PostMediaResponse toPostMedia(MediaAssetView asset, MediaDisplayType displayType) {
        return new PostMediaResponse(asset.assetId(), asset.publicId(), asset.mediaFormat(), asset.resourceType(),
                mediaAssets.transformDeliveryUrl(asset.url(), displayType),
                mediaAssets.transformDeliveryUrl(asset.secureUrl(), displayType), asset.displayName(),
                asset.width(), asset.height());
    }

    private PostMusicResponse resolveMusic(MusicTrackView track, Long start, Long end) {
        if (track == null) return null;
        String playbackUrl = track.songUrl();
        if (hasText(playbackUrl) && start != null && end != null && start >= 0 && end > start) {
            playbackUrl = mediaAssets.transformMusicUrl(playbackUrl, start, end);
        }
        return new PostMusicResponse(track.id(), track.displayName(), track.singleName(), track.displayImages(),
                playbackUrl, start, end, track.duration());
    }

    private FeedActorResponse toActor(String actorId, Map<String, UserIdentity> identities) {
        String cleanId = actorId == null ? "" : actorId.trim();
        UserIdentity actor = identity(cleanId, identities);
        return new FeedActorResponse(cleanId, actor.username(), firstNonBlank(actor.fullName(), actor.username(), cleanId),
                actor.avatarUrl());
    }

    private UserIdentity identity(String userId, Map<String, UserIdentity> identities) {
        String cleanId = userId == null ? "" : userId.trim();
        UserIdentity found = identities.get(cleanId);
        return found == null ? new UserIdentity(cleanId, cleanId, cleanId, "") : found;
    }

    private FeedActivityType parseActivityType(String value) {
        if (value == null || value.isBlank()) return FeedActivityType.ORIGINAL_POST;
        try { return FeedActivityType.valueOf(value.trim().toUpperCase(Locale.ROOT)); }
        catch (IllegalArgumentException ignored) { return FeedActivityType.ORIGINAL_POST; }
    }

    private String buildImpressionToken(String viewerId, String postId) {
        String material = (viewerId == null ? "" : viewerId.trim()) + ":" + postId + ":" + FEED_RANKING_VERSION;
        return FEED_RANKING_VERSION + ":" + UUID.nameUUIDFromBytes(material.getBytes(StandardCharsets.UTF_8));
    }

    private List<String> cleanIds(Collection<String> values) {
        return values == null ? List.of() : values.stream().filter(this::hasText).map(String::trim).distinct().toList();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) if (hasText(value)) return value.trim();
        return "";
    }

    private boolean hasText(String value) { return value != null && !value.isBlank(); }
}
