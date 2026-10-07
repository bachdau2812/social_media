package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.dto.response.FeedActorResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedMediaResponse;
import com.dauducbach.clone.modules.feed.dto.response.FeedResponse;
import com.dauducbach.clone.modules.feed.publicapi.FeedScreenQuery;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.publicapi.PostPresentationSnapshot;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class FeedScreenQueryService implements FeedScreenQuery {
    private final FeedService feedService;

    @Override
    public Mono<FeedSnapshot> getDiscoverFeed(
            String userId, int limit, MediaDisplayType mediaType, String cursor
    ) {
        return feedService.getFeed(userId, limit, mediaType, cursor).map(this::toSnapshot);
    }

    @Override
    public Mono<FeedSnapshot> getFriendsFeed(
            String userId, int limit, int page, MediaDisplayType mediaType
    ) {
        return feedService.getFriendsFeed(userId, limit, page, mediaType).map(this::toSnapshot);
    }

    private FeedSnapshot toSnapshot(FeedResponse response) {
        return new FeedSnapshot(response.userId(), response.limit(),
                response.items().stream().map(this::toSnapshot).toList(),
                response.hasMore(), response.nextCursor());
    }

    private FeedItemSnapshot toSnapshot(FeedItemResponse item) {
        return new FeedItemSnapshot(
                item.postId(), item.userId(), item.authorUsername(), item.authorFullName(),
                item.authorAvatarUrl(), item.content(), item.hashtags(), item.mediaRatio(),
                item.media().stream().map(this::toSnapshot).toList(), toSnapshot(item.music()),
                item.items().stream().map(this::toSnapshot).toList(), item.likeCount(),
                item.commentCount(), item.repostCount(), item.likedByCurrentUser(),
                item.repostedByCurrentUser(), item.createdAt(), item.updatedAt(), item.sourceType(),
                item.recommendationReason(), item.rankingVersion(), item.experimentId(),
                item.impressionToken(), item.feedEntryId(),
                item.activityType() == null ? null : item.activityType().name(), item.activityAt(),
                toSnapshot(item.reposter()));
    }

    private FeedMediaSnapshot toSnapshot(FeedMediaResponse media) {
        return new FeedMediaSnapshot(media.assetId(), media.publicId(), media.mediaFormat(),
                media.resourceType(), media.url(), media.secureUrl(), media.displayName());
    }

    private PostPresentationSnapshot.Item toSnapshot(PostItemResponse item) {
        return new PostPresentationSnapshot.Item(
                item.id(), item.orderNumber(), item.caption(), toSnapshot(item.media()), toSnapshot(item.music()));
    }

    private PostPresentationSnapshot.Media toSnapshot(PostMediaResponse media) {
        return media == null ? null : new PostPresentationSnapshot.Media(
                media.assetId(), media.publicId(), media.mediaFormat(), media.resourceType(), media.url(),
                media.secureUrl(), media.displayName(), media.width(), media.height());
    }

    private PostPresentationSnapshot.Music toSnapshot(PostMusicResponse music) {
        return music == null ? null : new PostPresentationSnapshot.Music(
                music.id(), music.displayName(), music.artist(), music.artworkUrl(), music.playbackUrl(),
                music.segmentStart(), music.segmentEnd(), music.duration());
    }

    private FeedActorSnapshot toSnapshot(FeedActorResponse actor) {
        return actor == null ? null : new FeedActorSnapshot(
                actor.id(), actor.username(), actor.displayName(), actor.avatarUrl());
    }
}
