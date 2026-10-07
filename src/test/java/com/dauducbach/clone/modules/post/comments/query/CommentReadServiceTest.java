package com.dauducbach.clone.modules.post.comments.query;

import com.dauducbach.clone.modules.media.publicapi.MediaUrlDelivery;
import com.dauducbach.clone.modules.post.comments.application.CommentCountCache;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.repository.CommentRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CommentReadServiceTest {
    @Mock CommentRepository commentRepository;
    @Mock CommentCountCache commentCountCache;
    @Mock R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock MediaUrlDelivery mediaAssets;

    @Test
    void rootCommentPageNormalizesPaginationAndIncludesReplyCounts() {
        Comment first = comment("comment-1", null);
        when(commentRepository.countRootByPostId("post-1")).thenReturn(Mono.just(12L));
        when(commentRepository.findRootByPostId("post-1", 10, 0)).thenReturn(Flux.just(first));
        when(commentRepository.countByParentId("comment-1")).thenReturn(Mono.just(2L));

        StepVerifier.create(newService().getRootCommentsPage("post-1", -1, -1))
                .expectNextMatches(page -> page.pageNumber() == 0
                        && page.totalElements() == 12
                        && page.totalPages() == 2
                        && page.content().get(0).getReplyCount() == 2)
                .verifyComplete();
    }

    @Test
    void countCommentsUsesTheCachePortBeforeDatabaseFallback() {
        when(commentCountCache.getPostCount(eq("post-1"), any())).thenReturn(Mono.just(8L));

        StepVerifier.create(newService().countCommentsByPostId("post-1"))
                .expectNext(8L)
                .verifyComplete();

        verify(commentRepository, never()).countByPostId("post-1");
    }

    @Test
    void commentedPostIdsReturnPaginatedResult() {
        when(commentRepository.countCommentedPostsByUserId("user-1")).thenReturn(Mono.just(3L));
        when(commentRepository.findCommentedPostIdsByUserId("user-1", 2, 0))
                .thenReturn(Flux.just("post-3", "post-2"));

        StepVerifier.create(newService().getCommentedPostIdsByUserId("user-1", 0, 2))
                .expectNextMatches(page -> page.content().equals(java.util.List.of("post-3", "post-2"))
                        && page.totalElements() == 3
                        && page.totalPages() == 2)
                .verifyComplete();
    }

    @Test
    void commenterIdsFilterBlanksAndDuplicates() {
        when(commentRepository.findDistinctUserIdsByPostId("post-1"))
                .thenReturn(Flux.just("user-1", " ", "user-1", "user-2"));

        StepVerifier.create(newService().getDistinctCommenterUserIdsByPostId("post-1"))
                .expectNext("user-1", "user-2")
                .verifyComplete();
    }

    private CommentReadService newService() {
        return new CommentReadService(commentRepository, commentCountCache, r2dbcEntityTemplate, mediaAssets);
    }

    private Comment comment(String id, String parentId) {
        return Comment.builder().id(id).postId("post-1").userId("user-1")
                .parentId(parentId).content("comment")
                .timestamp(Instant.parse("2026-06-08T00:00:00Z")).build();
    }
}
