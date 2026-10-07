package com.dauducbach.clone.modules.post.publicapi;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface PostInteractionQuery {
    Mono<Snapshot> findSnapshot(String postId, String viewerId);

    Flux<SnapshotEntry> findSnapshots(Collection<String> postIds, String viewerId);

    Mono<Long> countPostLikes(String postId);

    record Snapshot(long likes, long comments, long reposts, boolean likedByViewer, boolean repostedByViewer) { }
    record SnapshotEntry(String postId, Snapshot snapshot) { }
}
