package com.dauducbach.clone.modules.post.service;

import co.elastic.clients.json.JsonData;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class PostFeedQueryService {
    private static final Logger log = LoggerFactory.getLogger(PostFeedQueryService.class);
    private static final String APPROVED_STATUS = "APPROVED";
    private static final String POST_CONTENT_VECTOR_FIELD = "content_vector";

    PostDetailsRepository postDetailsRepository;
    ReactiveElasticsearchOperations elasticsearchOperations;

    public Mono<PostDetails> getApprovedPostById(String postId) {
        if (postId == null || postId.isBlank()) {
            return Mono.empty();
        }
        return postDetailsRepository.findApprovedFeedEligibleById(postId.trim())
                .filter(post -> APPROVED_STATUS.equalsIgnoreCase(post.getValidateStatus()));
    }
    public Flux<PostDetails> getRecentApprovedPosts(int limit, Set<String> excludedPostIds) {
        int safeLimit = Math.max(limit, 0);
        if (safeLimit == 0) {
            return Flux.empty();
        }

        Set<String> excludes = safeExcludedIds(excludedPostIds);
        return postDetailsRepository.findRecentApprovedPosts(safeLimit + excludes.size())
                .filter(post -> post.getPostId() != null && !excludes.contains(post.getPostId()))
                .take(safeLimit)
                .doOnComplete(() -> log.info("|PostFeedQueryService|getRecentApprovedPosts|limit={}|excluded={}",
                        safeLimit, excludes.size()));
    }

    public Flux<PostDetails> getRecentApprovedPostsFromMutualFriends(String userId, int limit, int offset) {
        int safeLimit = Math.max(limit, 0);
        if (safeLimit == 0) {
            return Flux.empty();
        }
        return postDetailsRepository.findRecentApprovedPostsFromMutualFriends(
                userId,
                safeLimit,
                Math.max(offset, 0)
        );
    }
    public Mono<List<String>> searchRecommendedPostIds(List<Double> queryVector, int limit, Set<String> excludedPostIds) {
        int safeLimit = Math.max(limit, 0);
        if (safeLimit == 0 || queryVector == null || queryVector.isEmpty()) {
            return Mono.just(List.of());
        }

        Set<String> excludes = safeExcludedIds(excludedPostIds);
        NativeQuery searchQuery = NativeQuery.builder()
                .withQuery(query -> query.scriptScore(scriptScore -> scriptScore
                        .query(inner -> inner.exists(exists -> exists.field(POST_CONTENT_VECTOR_FIELD)))
                        .script(script -> script
                                .lang("painless")
                                .source("cosineSimilarity(params.queryVector, '" + POST_CONTENT_VECTOR_FIELD + "') + 1.0")
                                .params("queryVector", JsonData.of(queryVector)))))
                .withMaxResults(safeLimit + excludes.size() + 20)
                .build();

        return elasticsearchOperations.search(searchQuery, PostVector.class)
                .map(SearchHit::getContent)
                .map(PostVector::getPostId)
                .filter(postId -> postId != null && !postId.isBlank())
                .filter(postId -> !excludes.contains(postId))
                .distinct()
                .concatMap(postId -> postDetailsRepository.findApprovedFeedEligibleById(postId)
                        .filter(post -> APPROVED_STATUS.equalsIgnoreCase(post.getValidateStatus()))
                        .map(PostDetails::getPostId))
                .take(safeLimit)
                .collectList()
                .doOnSuccess(ids -> log.info("|PostFeedQueryService|searchRecommendedPostIds|limit={}|resultCount={}",
                        safeLimit, ids.size()));
    }

    public Mono<List<Double>> getPostVector(String postId) {
        if (postId == null || postId.isBlank()) {
            return Mono.just(List.of());
        }

        return elasticsearchOperations.get(postId, PostVector.class)
                .map(PostVector::getContentVector)
                .filter(vector -> vector != null && !vector.isEmpty())
                .defaultIfEmpty(List.of())
                .onErrorResume(error -> {
                    log.warn("|PostFeedQueryService|getPostVector|failed|postId={}|error={}", postId, error.getMessage());
                    return Mono.just(List.of());
                });
    }

    private Set<String> safeExcludedIds(Set<String> excludedPostIds) {
        return excludedPostIds == null ? Set.of() : new HashSet<>(excludedPostIds);
    }
}
