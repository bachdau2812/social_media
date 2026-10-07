package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.dto.response.FeedItemResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.media.publicapi.MusicCatalog;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.publicapi.PostQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FeedItemHydratorTest {
    @Mock PostQuery postQuery;
    @Mock PostInteractionQuery postInteractionQuery;
    @Mock MediaCatalog mediaCatalog;
    @Mock MusicCatalog musicCatalog;
    @Mock MediaAssets mediaAssets;
    @Mock UserIdentityQuery userIdentityQuery;

    @InjectMocks FeedItemHydrator hydrator;

    @Test
    void hydratePageUsesBatchQueriesAndPreservesCandidateOrderWhenSomePostsAreUnavailable() {
        Instant now = Instant.parse("2026-07-31T00:00:00Z");
        List<String> candidates = List.of("post-2", "missing", "post-1");
        when(postQuery.findApprovedFeedSnapshots(candidates)).thenReturn(Flux.just(
                post("post-1", "author-1", now),
                post("post-2", "author-2", now)
        ));
        when(postInteractionQuery.findSnapshots(candidates, "viewer-1")).thenReturn(Flux.just(
                new PostInteractionQuery.SnapshotEntry("post-1",
                        new PostInteractionQuery.Snapshot(3, 4, 5, true, false)),
                new PostInteractionQuery.SnapshotEntry("post-2",
                        new PostInteractionQuery.Snapshot(6, 7, 8, false, true))
        ));
        when(userIdentityQuery.findIdentities(List.of("author-1", "author-2"))).thenReturn(Flux.just(
                new UserIdentity("author-1", "one", "Author One", "avatar-1"),
                new UserIdentity("author-2", "two", "Author Two", "avatar-2")
        ));
        when(mediaCatalog.findByOwnerIds(List.of("post-1", "post-2"), OwnerType.POST)).thenReturn(Flux.empty());
        MediaAssetView asset = new MediaAssetView("asset-1", "public-1", 640, 480,
                "jpg", "image", 10, "url", "secure-url", "post-1", OwnerType.POST,
                "1", "version-1", "image.jpg", now, now);
        when(mediaCatalog.findByIds(List.of("asset-1"))).thenReturn(Flux.just(asset));
        when(mediaAssets.transformDeliveryUrl("url", MediaDisplayType.FEED)).thenReturn("url");
        when(mediaAssets.transformDeliveryUrl("secure-url", MediaDisplayType.FEED)).thenReturn("secure-url");

        StepVerifier.create(hydrator.hydratePage("viewer-1", candidates, MediaDisplayType.FEED))
                .assertNext(items -> {
                    assertEquals(List.of("post-2", "post-1"), items.stream().map(FeedItemResponse::postId).toList());
                    assertEquals(6, items.get(0).likeCount());
                    assertEquals("Author Two", items.get(0).authorFullName());
                    assertEquals(3, items.get(1).likeCount());
                    assertEquals("asset-1", items.get(1).items().get(0).media().assetId());
                })
                .verifyComplete();

        verify(postQuery).findApprovedFeedSnapshots(candidates);
        verify(postInteractionQuery).findSnapshots(candidates, "viewer-1");
        verify(mediaCatalog).findByIds(List.of("asset-1"));
        verify(mediaCatalog).findByOwnerIds(List.of("post-1", "post-2"), OwnerType.POST);
        verify(userIdentityQuery).findIdentities(List.of("author-1", "author-2"));
    }

    private PostQuery.FeedPostSnapshot post(String postId, String ownerId, Instant now) {
        List<PostQuery.FeedItemSnapshot> items = postId.equals("post-1")
                ? List.of(new PostQuery.FeedItemSnapshot("item-1", 1, "caption", "asset-1", null, null, null))
                : List.of();
        return new PostQuery.FeedPostSnapshot(postId, ownerId, "content-" + postId,
                "[]", List.of(), "4:3", "APPROVED", null, null, null,
                now, now, items);
    }
}
