package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.post.PostFeedQueryService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
public class FeedCandidatePipeline {
    private static final Logger log = LoggerFactory.getLogger(FeedCandidatePipeline.class);
    static final String VECTOR_SOURCE = "vector";
    static final String RECENT_SOURCE = "recent";
    static final String VECTOR_REASON = "similar_to_your_interests";
    static final String RECENT_REASON = "recent_post";

    private final FeedVectorService feedVectorService;
    private final PostFeedQueryService postFeedQueryService;

    public Mono<List<FeedCandidate>> select(
            String userId,
            int limit,
            Set<String> excludedPostIds
    ) {
        int safeLimit = Math.max(limit, 0);
        if (safeLimit == 0) {
            return Mono.just(List.of());
        }

        Mono<List<String>> vectorCandidates = feedVectorService.buildQueryVector(userId)
                .flatMap(vector -> postFeedQueryService.searchRecommendedPostIds(vector, safeLimit, excludedPostIds))
                .onErrorResume(error -> {
                    log.warn("|FeedCandidatePipeline|select|vector source failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.just(List.of());
                });

        Mono<List<String>> recentCandidates = postFeedQueryService
                .getRecentApprovedPosts(safeLimit, excludedPostIds)
                .map(PostDetails::getPostId)
                .collectList()
                .onErrorResume(error -> {
                    log.warn("|FeedCandidatePipeline|select|recent source failed|userId={}|error={}",
                            userId, error.getMessage());
                    return Mono.just(List.of());
                });

        return Mono.zip(vectorCandidates, recentCandidates)
                .map(tuple -> rank(tuple.getT1(), tuple.getT2(), safeLimit));
    }

    List<FeedCandidate> rank(List<String> vectorIds, List<String> recentIds, int limit) {
        LinkedHashMap<String, CandidateFacts> deduplicated = new LinkedHashMap<>();
        add(deduplicated, vectorIds, VECTOR_SOURCE, VECTOR_REASON);
        add(deduplicated, recentIds, RECENT_SOURCE, RECENT_REASON);

        List<CandidateFacts> facts = new ArrayList<>(deduplicated.values())
                .stream()
                .limit(Math.max(limit, 0))
                .toList();
        int candidateCount = facts.size();
        List<FeedCandidate> ranked = new ArrayList<>(candidateCount);
        for (int index = 0; index < candidateCount; index++) {
            CandidateFacts candidate = facts.get(index);
            ranked.add(new FeedCandidate(
                    candidate.postId(),
                    candidate.sourceType(),
                    candidate.reason(),
                    FeedCandidateScorePolicy.score(candidateCount, index)
            ));
        }
        return List.copyOf(ranked);
    }

    private void add(
            LinkedHashMap<String, CandidateFacts> target,
            List<String> ids,
            String sourceType,
            String reason
    ) {
        if (ids == null) {
            return;
        }
        ids.stream()
                .filter(id -> id != null && !id.isBlank())
                .map(String::trim)
                .forEach(id -> target.putIfAbsent(id, new CandidateFacts(id, sourceType, reason)));
    }

    private record CandidateFacts(String postId, String sourceType, String reason) {
    }
}
