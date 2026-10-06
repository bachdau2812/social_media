package com.dauducbach.clone.modules.post.service.post;

import co.elastic.clients.json.JsonData;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.modules.post.dto.response.FriendFeedActivityResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.springframework.beans.factory.annotation.Value;
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
    private static final String POST_RECOMMENDATION_VECTOR_FIELD = "recommendation_vector";

    PostDetailsRepository postDetailsRepository;
    ReactiveElasticsearchOperations elasticsearchOperations;
    PostEmbeddingSourceReader sources;

    /** Rollback switches ranking and interaction reads to the retained raw content vector. */
    @NonFinal
    @Value("${vector.feed.recommendation.enabled:true}")
    boolean recommendationEnabled;

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

    public Flux<PostDetails> getApprovedFriendPostsBefore(String userId, java.time.Instant upperBound,
            java.time.Instant afterTime, String afterId, int limit) {
        if (limit <= 0) return Flux.empty();
        return postDetailsRepository.findApprovedFriendPostsBefore(userId, upperBound, afterTime, afterId, Math.min(limit, 40));
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

    public Flux<FriendFeedActivityResponse> getRecentFriendFeedActivities(String userId, int limit, int offset) {
        int safeLimit = Math.max(limit, 0);
        if (safeLimit == 0) {
            return Flux.empty();
        }
        return postDetailsRepository.findRecentFriendFeedActivities(
                        userId,
                        safeLimit,
                        Math.max(offset, 0)
                )
                .map(activity -> new FriendFeedActivityResponse(
                        activity.getFeedEntryId(),
                        activity.getPostId(),
                        activity.getActivityType(),
                        activity.getActorId(),
                        activity.getActivityAt()
                ));
    }
    public Mono<List<String>> searchRecommendedPostIds(List<Double> queryVector, int limit, Set<String> excludedPostIds) {
        int safeLimit = Math.max(limit, 0);
        if (safeLimit == 0 || queryVector == null || queryVector.isEmpty()) {
            return Mono.just(List.of());
        }

        Set<String> excludes = safeExcludedIds(excludedPostIds);
        NativeQuery searchQuery = NativeQuery.builder()
                .withQuery(query -> query.scriptScore(scriptScore -> scriptScore
                        .query(inner -> inner.bool(eligible -> eligible
                                .mustNot(q -> q.term(t -> t.field("deleted").value(true)))
                                .filter(q -> q.term(t -> t.field("model").value(VectorMath.MODEL)))
                                .filter(q -> q.term(t -> t.field("dimension").value(VectorMath.DIMENSION)))
                                .filter(q -> q.term(t -> t.field("schema_version").value(VectorMath.SCHEMA_VERSION)))
                                .should(q -> q.exists(e -> e.field(recommendationEnabled ? POST_RECOMMENDATION_VECTOR_FIELD : POST_CONTENT_VECTOR_FIELD)))
                                .should(q -> q.exists(e -> e.field(POST_CONTENT_VECTOR_FIELD))).minimumShouldMatch("1")))
                        .script(script -> script
                                .lang("painless")
                                .source(recommendationEnabled
                                        ? "(doc['recommendation_vector'].size() != 0 ? cosineSimilarity(params.queryVector, 'recommendation_vector') : cosineSimilarity(params.queryVector, 'content_vector')) + 1.0"
                                        : "cosineSimilarity(params.queryVector, 'content_vector') + 1.0")
                                .params("queryVector", JsonData.of(VectorMath.normalize(queryVector))))))
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

    /** Empty means a permanent skip; missing/currently pending vectors fail so consumers can retry. */
    public Mono<List<Double>> getPostRecommendationVector(String postId) {
        if (postId == null || postId.isBlank()) {
            return Mono.just(List.of());
        }

        return sources.load(postId).map(java.util.Optional::of).defaultIfEmpty(java.util.Optional.empty()).flatMap(current -> {
            if (current.isEmpty() || "REJECTED".equalsIgnoreCase(current.get().post().getValidateStatus())) return Mono.just(List.of());
            if (!"APPROVED".equalsIgnoreCase(current.get().post().getValidateStatus()))
                return Mono.error(new PostVectorPendingException(postId, "moderation pending"));
            return elasticsearchOperations.get(postId, PostVector.class)
                    .switchIfEmpty(Mono.error(new PostVectorPendingException(postId, "approved vector missing")))
                    .flatMap(doc -> {
                        if (Boolean.TRUE.equals(doc.getDeleted())) return Mono.just(List.of());
                        try {
                            VectorMath.requireCompatible(doc.getModel(), doc.getDimension(), doc.getSchemaVersion());
                            boolean legacy = doc.getSourceRevision() == null && doc.getEmbeddingState() == null && doc.getRecommendationVector() == null;
                            if (!legacy && !java.util.Objects.equals(current.get().revision(), doc.getSourceRevision()))
                                return Mono.error(new PostVectorPendingException(postId, "source revision not built"));
                            if ("SKIPPED_NO_INPUT".equals(doc.getEmbeddingState())) return Mono.just(List.of());
                            if (!legacy && !"READY".equals(doc.getEmbeddingState()))
                                return Mono.error(new PostVectorPendingException(postId, "embedding state not ready"));
                            List<Double> vector = recommendationEnabled ? doc.getRecommendationVector() : doc.getContentVector();
                            if (recommendationEnabled && (vector == null || vector.isEmpty())) vector = doc.getContentVector();
                            if (!recommendationEnabled && (vector == null || vector.isEmpty())) return Mono.just(List.of());
                            return Mono.just(VectorMath.normalize(vector));
                        } catch (IllegalArgumentException error) {
                            return Mono.error(new PostVectorPendingException(postId, "unknown/incompatible/malformed vector; repair required"));
                        }
                    });
        });
    }

    /** Compatibility adapter used by the current ST/LT consumers; same strict readiness contract. */
    public Mono<List<Double>> getPostVector(String postId) { return getPostRecommendationVector(postId); }

    private Set<String> safeExcludedIds(Set<String> excludedPostIds) {
        return excludedPostIds == null ? Set.of() : new HashSet<>(excludedPostIds);
    }
}
