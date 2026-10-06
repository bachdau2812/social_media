package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.feed.dto.FeedSourceCursor;
import com.dauducbach.clone.modules.post.dto.response.PopularPostRef;
import com.dauducbach.clone.modules.post.service.post.PopularPostQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.List;

@Component
@RequiredArgsConstructor
public class PopularFeedCandidateSource {
    private final PopularPostQueryService posts;

    public Mono<List<FeedCandidate>> find(Instant upperBound, Instant now, FeedSourceCursor after, int limit) {
        return posts.findPopularPosts(upperBound, now,
                        after == null ? null : new PopularPostRef(after.postId(), after.timeMs()), limit)
                .map(refs -> refs.stream().map(ref -> new FeedCandidate(ref.postId(), "POPULAR", "popular_post",
                        0, ref.popularSinceMillis())).toList());
    }
}
