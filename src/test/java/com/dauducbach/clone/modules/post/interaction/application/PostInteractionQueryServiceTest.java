package com.dauducbach.clone.modules.post.interaction.application;

import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.post.publicapi.CommentQuery;
import com.dauducbach.clone.modules.post.service.post.LikeService;
import com.dauducbach.clone.modules.post.service.post.RepostService;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.projection.PostInteractionSnapshotProjection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.util.List;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostInteractionQueryServiceTest {
    @Mock LikeService likes;
    @Mock CommentQuery comments;
    @Mock RepostService reposts;
    @Mock PostDetailsRepository postDetails;

    @Test
    void combinesPostCountsAndViewerStateBehindOneQuery() {
        when(likes.countLikes("post-1", EntityType.POST.name())).thenReturn(Mono.just(11L));
        when(comments.countByPostId("post-1")).thenReturn(Mono.just(4L));
        when(reposts.countReposts("post-1")).thenReturn(Mono.just(2L));
        when(likes.hasLiked("viewer-1", "post-1", EntityType.POST.name())).thenReturn(Mono.just(true));
        when(reposts.hasReposted("viewer-1", "post-1")).thenReturn(Mono.just(false));
        var query = new PostInteractionQueryService(likes, comments, reposts, postDetails);

        StepVerifier.create(query.findSnapshot("post-1", "viewer-1"))
                .expectNext(new PostInteractionQuery.Snapshot(11, 4, 2, true, false))
                .verifyComplete();
    }

    @Test
    void omitsViewerLookupsForAnonymousSnapshot() {
        when(likes.countLikes("post-1", EntityType.POST.name())).thenReturn(Mono.just(0L));
        when(comments.countByPostId("post-1")).thenReturn(Mono.just(0L));
        when(reposts.countReposts("post-1")).thenReturn(Mono.just(0L));
        var query = new PostInteractionQueryService(likes, comments, reposts, postDetails);

        StepVerifier.create(query.findSnapshot("post-1", null))
                .expectNext(new PostInteractionQuery.Snapshot(0, 0, 0, false, false))
                .verifyComplete();

        verify(likes, never()).hasLiked("", "post-1", EntityType.POST.name());
        verify(reposts, never()).hasReposted("", "post-1");
    }

    @Test
    void batchSnapshotUsesOnePostOwnedReadAndKeepsEachPostViewerState() {
        PostInteractionSnapshotProjection first = row("post-1", 11, 4, 2, true, false);
        PostInteractionSnapshotProjection second = row("post-2", 0, 3, 1, false, true);
        when(postDetails.findInteractionSnapshots(List.of("post-1", "post-2"), "viewer-1"))
                .thenReturn(Flux.just(first, second));
        var query = new PostInteractionQueryService(likes, comments, reposts, postDetails);

        StepVerifier.create(query.findSnapshots(List.of("post-1", "post-2"), "viewer-1"))
                .expectNext(new PostInteractionQuery.SnapshotEntry("post-1",
                        new PostInteractionQuery.Snapshot(11, 4, 2, true, false)))
                .expectNext(new PostInteractionQuery.SnapshotEntry("post-2",
                        new PostInteractionQuery.Snapshot(0, 3, 1, false, true)))
                .verifyComplete();
    }

    private PostInteractionSnapshotProjection row(
            String postId, long likesCount, long commentsCount, long repostCount, boolean liked, boolean reposted) {
        PostInteractionSnapshotProjection row = org.mockito.Mockito.mock(PostInteractionSnapshotProjection.class);
        when(row.getPostId()).thenReturn(postId);
        when(row.getLikes()).thenReturn(likesCount);
        when(row.getComments()).thenReturn(commentsCount);
        when(row.getReposts()).thenReturn(repostCount);
        when(row.getLikedByViewer()).thenReturn(liked);
        when(row.getRepostedByViewer()).thenReturn(reposted);
        return row;
    }
}
