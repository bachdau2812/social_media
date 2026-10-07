package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.query.PostContentQueryService;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.publicapi.PostPresentationSnapshot;
import com.dauducbach.clone.modules.post.publicapi.PostProfileQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PostProfileQueryService implements PostProfileQuery {
    private final PostContentQueryService postContentQueryService;
    private final PostDetailQueryService postDetailQueryService;
    private final RepostService repostService;
    private final PostInteractionQuery postInteractionQuery;
    private final UserIdentityQuery userIdentityQueryService;

    @Override
    public Flux<PostProfileQuery.ProfilePostSnapshot> getRecentPosts(String viewerId, String userId, int limit) {
        return postContentQueryService.findByAuthorId(userId, 0, limit)
                .concatMap(post -> hydrate(viewerId, post));
    }

    @Override
    public Flux<PostProfileQuery.ProfilePostSnapshot> getRepostedPosts(String viewerId, String userId, int limit) {
        return repostService.getRepostedPosts(userId, limit)
                .concatMap(post -> hydrate(viewerId, post));
    }

    private Mono<PostProfileQuery.ProfilePostSnapshot> hydrate(String viewerId, PostDetails post) {
        String postId = post.getPostId();
        Mono<UserIdentity> author =
                userIdentityQueryService.resolveIdentity(post.getUserId());
        Mono<Optional<PostPresentationSnapshot.Item>> firstItem = postDetailQueryService
                .getFirstItem(post, MediaDisplayType.POST)
                .map(this::toSnapshotItem)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .onErrorReturn(Optional.empty());
        Mono<PostInteractionQuery.Snapshot> interactions = postInteractionQuery
                .findSnapshot(postId, viewerId)
                .onErrorReturn(new PostInteractionQuery.Snapshot(0, 0, 0, false, false));

        return Mono.zip(author, firstItem, interactions)
                .map(tuple -> new PostProfileQuery.ProfilePostSnapshot(
                        postId,
                        post.getUserId(),
                        tuple.getT1().username(),
                        tuple.getT1().fullName(),
                        tuple.getT1().avatarUrl(),
                        post.getContent(),
                        post.getHashtagList(),
                        PostMediaRatio.defaultIfMissing(post.getMediaRatio()),
                        tuple.getT2().orElse(null),
                        null,
                        tuple.getT3().likes(),
                        tuple.getT3().comments(),
                        tuple.getT3().reposts(),
                        tuple.getT3().likedByViewer(),
                        tuple.getT3().repostedByViewer(),
                        post.getCreatedAt(),
                        post.getUpdatedAt()
                ));
    }

    private PostPresentationSnapshot.Item toSnapshotItem(PostItemResponse item) {
        return new PostPresentationSnapshot.Item(
                item.id(), item.orderNumber(), item.caption(),
                item.media() == null ? null : new PostPresentationSnapshot.Media(
                        item.media().assetId(), item.media().publicId(), item.media().mediaFormat(),
                        item.media().resourceType(), item.media().url(), item.media().secureUrl(),
                        item.media().displayName(), item.media().width(), item.media().height()),
                toSnapshotMusic(item.music()));
    }

    private PostPresentationSnapshot.Music toSnapshotMusic(PostMusicResponse music) {
        return music == null ? null : new PostPresentationSnapshot.Music(
                music.id(), music.displayName(), music.artist(), music.artworkUrl(), music.playbackUrl(),
                music.segmentStart(), music.segmentEnd(), music.duration());
    }
}
