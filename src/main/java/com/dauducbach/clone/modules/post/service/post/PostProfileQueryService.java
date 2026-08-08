package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.service.comment.CommentService;
import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.constant.PostMediaRatio;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PostProfileQueryService {
    private final PostService postService;
    private final PostDetailQueryService postDetailQueryService;
    private final LikeService likeService;
    private final CommentService commentService;
    private final RepostService repostService;
    private final UserIdentityQueryService userIdentityQueryService;

    public Flux<ProfilePostSnapshot> getRecentPosts(String viewerId, String userId, int limit) {
        return postService.getPostsByUserId(userId, 0, limit)
                .concatMap(post -> hydrate(viewerId, post));
    }

    public Flux<ProfilePostSnapshot> getRepostedPosts(String viewerId, String userId, int limit) {
        return repostService.getRepostedPosts(userId, limit)
                .concatMap(post -> hydrate(viewerId, post));
    }

    private Mono<ProfilePostSnapshot> hydrate(String viewerId, PostDetails post) {
        String postId = post.getPostId();
        Mono<UserIdentityQueryService.IdentitySnapshot> author =
                userIdentityQueryService.resolveIdentity(post.getUserId());
        Mono<Optional<PostItemResponse>> firstItem = postDetailQueryService
                .getFirstItem(post, MediaDisplayType.POST)
                .map(Optional::of)
                .defaultIfEmpty(Optional.empty())
                .onErrorReturn(Optional.empty());
        Mono<Long> likeCount = likeService.countLikes(postId, EntityType.POST.name()).onErrorReturn(0L);
        Mono<Long> commentCount = commentService.countCommentsByPostId(postId).onErrorReturn(0L);
        Mono<Long> repostCount = repostService.countReposts(postId).onErrorReturn(0L);
        Mono<Boolean> liked = likeService.hasLiked(viewerId, postId, EntityType.POST.name()).onErrorReturn(false);
        Mono<Boolean> reposted = repostService.hasReposted(viewerId, postId).onErrorReturn(false);

        return Mono.zip(author, firstItem, likeCount, commentCount, repostCount, liked, reposted)
                .map(tuple -> new ProfilePostSnapshot(
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
                        tuple.getT3(),
                        tuple.getT4(),
                        tuple.getT5(),
                        tuple.getT6(),
                        tuple.getT7(),
                        post.getCreatedAt(),
                        post.getUpdatedAt()
                ));
    }

    public record ProfilePostSnapshot(
            String postId,
            String userId,
            String authorUsername,
            String authorFullName,
            String authorAvatarUrl,
            String content,
            List<String> hashtags,
            String mediaRatio,
            PostItemResponse firstItem,
            PostMusicResponse music,
            long likeCount,
            long commentCount,
            long repostCount,
            boolean likedByCurrentUser,
            boolean repostedByCurrentUser,
            Instant createdAt,
            Instant updatedAt
    ) {
        public ProfilePostSnapshot {
            hashtags = hashtags == null ? List.of() : List.copyOf(hashtags);
        }
    }
}
