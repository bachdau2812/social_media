package com.dauducbach.clone.modules.post.query;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.PostItem;
import com.dauducbach.clone.modules.post.publicapi.PostQuery;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repository.PostItemRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostQueryServiceTest {
    @Test
    void feedSnapshotLoadsApprovedContentAndItemsInBatches() {
        PostContentQueryService content = mock(PostContentQueryService.class);
        PostDetailsRepository posts = mock(PostDetailsRepository.class);
        PostItemRepository postItems = mock(PostItemRepository.class);
        Instant now = Instant.parse("2026-07-31T00:00:00Z");
        PostDetails post = PostDetails.builder().postId("post-1").userId("author-1")
                .content("content").hashtag("[\"tag\"]").mediaRatio("4:3")
                .validateStatus("APPROVED").createdAt(now).updatedAt(now).build();
        PostItem item = PostItem.builder().id("item-1").postId("post-1").orderNumber(1)
                .mediaId("asset-1").caption("caption").build();
        when(posts.findApprovedFeedEligibleByIdIn(List.of("post-1", "post-2"))).thenReturn(Flux.just(post));
        when(postItems.findByPostIdInOrderByPostIdAscOrderNumberAsc(List.of("post-1")))
                .thenReturn(Flux.just(item));
        PostQuery query = new PostQueryService(content, posts, postItems);

        StepVerifier.create(query.findApprovedFeedSnapshots(List.of("post-1", "post-2")))
                .expectNext(new PostQuery.FeedPostSnapshot("post-1", "author-1", "content", "[\"tag\"]",
                        List.of("tag"), "4:3", "APPROVED", null, null, null, now, now,
                        List.of(new PostQuery.FeedItemSnapshot("item-1", 1, "caption", "asset-1", null, null, null))))
                .verifyComplete();

        verify(posts).findApprovedFeedEligibleByIdIn(List.of("post-1", "post-2"));
        verify(postItems).findByPostIdInOrderByPostIdAscOrderNumberAsc(List.of("post-1"));
    }

    @Test
    void emptyFeedSnapshotDoesNotQueryPostItems() {
        PostDetailsRepository posts = mock(PostDetailsRepository.class);
        PostItemRepository postItems = mock(PostItemRepository.class);
        PostQuery query = new PostQueryService(mock(PostContentQueryService.class), posts, postItems);

        StepVerifier.create(query.findApprovedFeedSnapshots(List.of()))
                .verifyComplete();

        org.mockito.Mockito.verifyNoInteractions(posts, postItems);
    }
}
