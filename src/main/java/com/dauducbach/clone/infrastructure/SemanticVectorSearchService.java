package com.dauducbach.clone.infrastructure;

import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.modules.user.entity.UserDetailVector;
import com.dauducbach.clone.utils.GetVectorEmbedding;
import com.dauducbach.clone.infrastructure.vector.VectorMath;
import co.elastic.clients.json.JsonData;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class SemanticVectorSearchService {
    static final float MIN_VECTOR_SIMILARITY = 0.80f;
    private static final float ELASTIC_MIN_SCORE = MIN_VECTOR_SIMILARITY + 1.0f;
    private static final String USER_LONG_TERM_VECTOR_FIELD = "user_long_term_vector";
    private static final String POST_CONTENT_VECTOR_FIELD = "content_vector";

    GetVectorEmbedding getVectorEmbedding;
    ReactiveElasticsearchOperations elasticsearchOperations;

    public Mono<List<String>> searchUserIds(String query, int limit, Set<String> excludedIds) {
        return semanticSearch(
                query,
                limit,
                excludedIds,
                USER_LONG_TERM_VECTOR_FIELD,
                UserDetailVector.class,
                UserDetailVector::getUserId
        );
    }

    public Mono<List<String>> searchUserIdsByVector(List<Double> vector,
                                                    int limit,
                                                    Set<String> excludedIds) {
        if (vector == null || vector.isEmpty() || limit <= 0) {
            return Mono.just(List.of());
        }

        Set<String> safeExcludedIds = excludedIds == null ? Set.of() : new HashSet<>(excludedIds);
        int maxResults = limit + safeExcludedIds.size() + 10;
        return searchByVector(vector, USER_LONG_TERM_VECTOR_FIELD, UserDetailVector.class, maxResults)
                .map(SearchHit::getContent)
                .map(UserDetailVector::getUserId)
                .filter(id -> id != null && !id.isBlank())
                .filter(id -> !safeExcludedIds.contains(id))
                .distinct()
                .take(limit)
                .collectList();
    }

    public Mono<List<String>> searchPostIds(String query, int limit, Set<String> excludedIds) {
        return semanticSearch(
                query,
                limit,
                excludedIds,
                POST_CONTENT_VECTOR_FIELD,
                PostVector.class,
                PostVector::getPostId
        );
    }

    private <T> Mono<List<String>> semanticSearch(String query,
                                                  int limit,
                                                  Set<String> excludedIds,
                                                  String vectorField,
                                                  Class<T> entityClass,
                                                  Function<T, String> idExtractor) {
        if (limit <= 0) {
            return Mono.just(List.of());
        }

        Set<String> safeExcludedIds = excludedIds == null ? Set.of() : new HashSet<>(excludedIds);

        return getVectorEmbedding.getEmbedding(query)
                .flatMapMany(vector -> searchByVector(vector, vectorField, entityClass, limit + safeExcludedIds.size() + 10))
                .map(SearchHit::getContent)
                .map(idExtractor)
                .filter(id -> id != null && !id.isBlank())
                .filter(id -> !safeExcludedIds.contains(id))
                .distinct()
                .take(limit)
                .collectList();
    }

    private <T> Flux<SearchHit<T>> searchByVector(List<Double> vector,
                                                  String vectorField,
                                                  Class<T> entityClass,
                                                  int maxResults) {
        boolean userVector = USER_LONG_TERM_VECTOR_FIELD.equals(vectorField);
        String modelField = userVector ? vectorField + "_model" : "model";
        String dimensionField = userVector ? vectorField + "_dimension" : "dimension";
        String schemaField = userVector ? vectorField + "_schema_version" : "schema_version";
        NativeQuery searchQuery = NativeQuery.builder()
                .withQuery(query -> query.scriptScore(scriptScore -> scriptScore
                        .query(inner -> inner.bool(eligible -> eligible
                                .must(q -> q.exists(exists -> exists.field(vectorField)))
                                .mustNot(q -> q.term(t -> t.field("deleted").value(true)))
                                .filter(q -> q.term(t -> t.field(modelField).value(VectorMath.MODEL)))
                                .filter(q -> q.term(t -> t.field(dimensionField).value(VectorMath.DIMENSION)))
                                .filter(q -> q.term(t -> t.field(schemaField).value(VectorMath.SCHEMA_VERSION)))))
                        .script(script -> script
                                .lang("painless")
                                .source("cosineSimilarity(params.queryVector, '" + vectorField + "') + 1.0")
                                .params("queryVector", JsonData.of(VectorMath.normalize(vector))))
                        .minScore(ELASTIC_MIN_SCORE)))
                .withMinScore(ELASTIC_MIN_SCORE)
                .withMaxResults(Math.max(maxResults, 1))
                .build();

        return elasticsearchOperations.search(searchQuery, entityClass);
    }
}
