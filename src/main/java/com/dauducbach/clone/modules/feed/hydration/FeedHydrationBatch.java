package com.dauducbach.clone.modules.feed.hydration;

import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MusicTrackView;
import com.dauducbach.clone.modules.post.publicapi.PostInteractionQuery;
import com.dauducbach.clone.modules.post.publicapi.PostQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;

import java.util.List;
import java.util.Map;

/** Batched, module-owned read models needed to compose one feed page. */
public record FeedHydrationBatch(
        Map<String, PostQuery.FeedPostSnapshot> postsById,
        Map<String, PostInteractionQuery.Snapshot> interactionsByPostId,
        Map<String, UserIdentity> identitiesByUserId,
        Map<String, MediaAssetView> assetsById,
        Map<String, List<MediaAssetView>> mediaByOwnerId,
        Map<String, MusicTrackView> musicById) {
    public FeedHydrationBatch {
        postsById = Map.copyOf(postsById);
        interactionsByPostId = Map.copyOf(interactionsByPostId);
        identitiesByUserId = Map.copyOf(identitiesByUserId);
        assetsById = Map.copyOf(assetsById);
        mediaByOwnerId = mediaByOwnerId.entrySet().stream()
                .collect(java.util.stream.Collectors.toUnmodifiableMap(Map.Entry::getKey,
                        entry -> List.copyOf(entry.getValue())));
        musicById = Map.copyOf(musicById);
    }
}
