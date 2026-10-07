package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMusicResponse;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.query.PostContentQueryService;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PostProfileQueryServiceTest {
    @Test
    void hydratesProfilePostFromOwnedContentAndInteractionQueries() {
        PostContentQueryService contentQuery = mock(PostContentQueryService.class);
        PostDetailQueryService detailQuery = mock(PostDetailQueryService.class);
        RepostService repostService = mock(RepostService.class);
        PostInteractionQuery interactionQuery = mock(PostInteractionQuery.class);
        UserIdentityQuery identityQuery = mock(UserIdentityQuery.class);
        Instant createdAt = Instant.parse("2025-04-01T10:15:30Z");
        PostDetails post = PostDetails.builder()
                .postId("post-1")
                .userId("author-1")
                .content("hello")
                .createdAt(createdAt)
                .updatedAt(createdAt)
                .mediaRatio("16:9")
                .build();

        when(contentQuery.findByAuthorId("author-1", 0, 5)).thenReturn(Flux.just(post));
        when(identityQuery.resolveIdentity("author-1"))
                .thenReturn(Mono.just(new UserIdentity("author-1", "author", "Author", "avatar")));
        when(detailQuery.getFirstItem(post, com.dauducbach.clone.modules.media.constant.MediaDisplayType.POST))
                .thenReturn(Mono.just(new PostItemResponse(
                        "item-1", 0, "caption",
                        new PostMediaResponse("asset-1", "public-1", "jpg", "image", "url", "secure", "photo", 640, 480),
                        new PostMusicResponse("music-1", "Track", "Artist", "art", "play", 1L, 8L, 9L))));
        when(interactionQuery.findSnapshot("post-1", "viewer-1"))
                .thenReturn(Mono.just(new PostInteractionQuery.Snapshot(7, 3, 2, true, false)));

        PostProfileQueryService query = new PostProfileQueryService(
                contentQuery, detailQuery, repostService, interactionQuery, identityQuery);

        StepVerifier.create(query.getRecentPosts("viewer-1", "author-1", 5))
                .assertNext(snapshot -> {
                    org.junit.jupiter.api.Assertions.assertEquals("post-1", snapshot.postId());
                    org.junit.jupiter.api.Assertions.assertEquals("author", snapshot.authorUsername());
                    org.junit.jupiter.api.Assertions.assertEquals("16:9", snapshot.mediaRatio());
                    org.junit.jupiter.api.Assertions.assertEquals(7, snapshot.likeCount());
                    org.junit.jupiter.api.Assertions.assertEquals(3, snapshot.commentCount());
                    org.junit.jupiter.api.Assertions.assertEquals(2, snapshot.repostCount());
                    org.junit.jupiter.api.Assertions.assertTrue(snapshot.likedByCurrentUser());
                    org.junit.jupiter.api.Assertions.assertFalse(snapshot.repostedByCurrentUser());
                    org.junit.jupiter.api.Assertions.assertEquals("item-1", snapshot.firstItem().id());
                    org.junit.jupiter.api.Assertions.assertEquals("asset-1", snapshot.firstItem().media().assetId());
                    org.junit.jupiter.api.Assertions.assertEquals("music-1", snapshot.firstItem().music().id());
                })
                .verifyComplete();
    }
}
