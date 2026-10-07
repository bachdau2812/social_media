package com.dauducbach.clone.modules.post.publicapi;

import com.dauducbach.clone.modules.post.dto.response.FriendFeedActivityResponse;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Set;

/** Read contracts used by feed candidate selection; persistence entities stay inside post. */
public interface PostFeedQuery {
    Flux<String> findRecentApprovedPostIds(int limit, Set<String> excludedPostIds);

    Flux<FeedCandidatePost> findApprovedFriendPostsBefore(
            String userId, Instant upperBound, Instant afterTime, String afterId, int limit);

    Flux<FriendFeedActivityResponse> findRecentFriendFeedActivities(String userId, int limit, int offset);

    Mono<List<String>> searchRecommendedPostIds(List<Double> queryVector, int limit, Set<String> excludedPostIds);

    Mono<List<Double>> getRecommendationVector(String postId);

    record FeedCandidatePost(String postId, Instant createdAt) { }
}
