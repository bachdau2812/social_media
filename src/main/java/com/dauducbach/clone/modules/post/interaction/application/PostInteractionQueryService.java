package com.dauducbach.clone.modules.post.interaction.application;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.publicapi.CommentQuery;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.service.post.LikeService;
import com.dauducbach.clone.modules.post.service.post.RepostService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;

@Service
@RequiredArgsConstructor
public class PostInteractionQueryService implements PostInteractionQuery {
    private final LikeService likeService;
    private final CommentQuery commentQuery;
    private final RepostService repostService;
    private final PostDetailsRepository postDetailsRepository;

    @Override
    public Mono<Long> countPostLikes(String postId) {
        return likeService.countLikes(requirePostId(postId), EntityType.POST.name()).defaultIfEmpty(0L);
    }

    @Override
    public Mono<Snapshot> findSnapshot(String postId, String viewerId) {
        String cleanPostId = requirePostId(postId);
        boolean hasViewer = viewerId != null && !viewerId.isBlank();
        String cleanViewerId = hasViewer ? viewerId.trim() : "";
        return Mono.zip(
                        countPostLikes(cleanPostId),
                        commentQuery.countByPostId(cleanPostId).defaultIfEmpty(0L),
                        repostService.countReposts(cleanPostId).defaultIfEmpty(0L),
                        hasViewer
                                ? likeService.hasLiked(cleanViewerId, cleanPostId, EntityType.POST.name()).defaultIfEmpty(false)
                                : Mono.just(false),
                        hasViewer
                                ? repostService.hasReposted(cleanViewerId, cleanPostId).defaultIfEmpty(false)
                                : Mono.just(false))
                .map(state -> new Snapshot(state.getT1(), state.getT2(), state.getT3(), state.getT4(), state.getT5()));
    }

    @Override
    public reactor.core.publisher.Flux<SnapshotEntry> findSnapshots(Collection<String> postIds, String viewerId) {
        List<String> ids = postIds == null ? List.of() : postIds.stream()
                .filter(id -> id != null && !id.isBlank()).map(String::trim).distinct().toList();
        if (ids.isEmpty()) return reactor.core.publisher.Flux.empty();
        String cleanViewerId = viewerId == null ? "" : viewerId.trim();
        return postDetailsRepository.findInteractionSnapshots(ids, cleanViewerId)
                .map(row -> new SnapshotEntry(row.getPostId(), new Snapshot(
                        value(row.getLikes()), value(row.getComments()), value(row.getReposts()),
                        Boolean.TRUE.equals(row.getLikedByViewer()), Boolean.TRUE.equals(row.getRepostedByViewer()))));
    }

    private long value(Long value) { return value == null ? 0L : value; }

    private String requirePostId(String postId) {
        if (postId == null || postId.isBlank()) {
            throw new AppException(ErrorCode.POST_FETCH_FAILED, "postId is required");
        }
        return postId.trim();
    }
}
