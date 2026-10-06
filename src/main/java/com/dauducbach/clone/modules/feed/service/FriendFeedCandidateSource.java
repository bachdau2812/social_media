package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.dto.FeedSourceCursor;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
public class FriendFeedCandidateSource {
    private final PostFeedQueryService posts;

    public Mono<List<FeedCandidate>> find(String viewer, Instant upperBound, FeedSourceCursor after, int limit) {
        return posts.getApprovedFriendPostsBefore(viewer, upperBound,
                        after == null ? null : after.orderTime(), after == null ? null : after.postId(), limit)
                .map(post -> new FeedCandidate(post.getPostId(), "FRIENDS", "friend_post", 0,
                        post.getCreatedAt().toEpochMilli(), post.getCreatedAt()))
                .collectList();
    }
}
