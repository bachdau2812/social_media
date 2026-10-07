package com.dauducbach.clone.modules.semanticsearch.infrastructure.elasticsearch;

import co.elastic.clients.json.JsonData;
import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.modules.embedding.publicapi.TextEmbeddingProvider;
import com.dauducbach.clone.modules.semanticsearch.publicapi.SemanticVectorSearch;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Elasticsearch adapter for the stable user and post semantic search contracts. */
@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ElasticsearchSemanticVectorSearch implements SemanticVectorSearch {
    static final float MIN_VECTOR_SIMILARITY = 0.80f;
    private static final float MIN_USER_DISCOVERY_SIMILARITY = 0.50f;

    TextEmbeddingProvider embeddings;
    ReactiveElasticsearchOperations elasticsearch;

    @Override
    public Mono<List<String>> searchUserIds(String query, int limit, Set<String> excludedIds) {
        return semanticSearch(query, limit, excludedIds, Target.USERS);
    }

    @Override
    public Mono<List<String>> searchUserIdsByVector(List<Double> vector, int limit, Set<String> excludedIds) {
        if (vector == null || vector.isEmpty() || limit <= 0) return Mono.just(List.of());
        Set<String> excluded = safeExcluded(excludedIds);
        return searchByVector(vector, Target.USERS, limit + excluded.size() + 10, MIN_USER_DISCOVERY_SIMILARITY)
                .map(SearchHit::getId)
                .filter(this::hasId)
                .filter(id -> !excluded.contains(id))
                .distinct()
                .take(limit)
                .collectList();
    }

    @Override
    public Mono<List<String>> searchPostIds(String query, int limit, Set<String> excludedIds) {
        return semanticSearch(query, limit, excludedIds, Target.POSTS);
    }

    private Mono<List<String>> semanticSearch(String query, int limit, Set<String> excludedIds, Target target) {
        if (limit <= 0) return Mono.just(List.of());
        Set<String> excluded = safeExcluded(excludedIds);
        return embeddings.getEmbedding(query)
                .flatMapMany(vector -> searchByVector(vector, target, limit + excluded.size() + 10, MIN_VECTOR_SIMILARITY))
                .map(SearchHit::getId)
                .filter(this::hasId)
                .filter(id -> !excluded.contains(id))
                .distinct()
                .take(limit)
                .collectList();
    }

    private Flux<SearchHit<Map>> searchByVector(List<Double> vector, Target target, int maxResults, float minSimilarity) {
        float minScore = minSimilarity + 1.0f;
        NativeQuery query = NativeQuery.builder()
                .withQuery(root -> root.scriptScore(scriptScore -> scriptScore
                        .query(inner -> inner.bool(eligible -> eligible
                                .must(q -> q.exists(exists -> exists.field(target.vectorField)))
                                .mustNot(q -> q.term(term -> term.field("deleted").value(true)))
                                .filter(q -> q.term(term -> term.field(target.modelField).value(VectorMath.MODEL)))
                                .filter(q -> q.term(term -> term.field(target.dimensionField).value(VectorMath.DIMENSION)))
                                .filter(q -> q.term(term -> term.field(target.schemaVersionField).value(VectorMath.SCHEMA_VERSION)))))
                        .script(script -> script
                                .lang("painless")
                                .source("cosineSimilarity(params.queryVector, '" + target.vectorField + "') + 1.0")
                                .params("queryVector", JsonData.of(VectorMath.normalize(vector))))
                        .minScore(minScore)))
                .withMinScore(minScore)
                .withMaxResults(Math.max(maxResults, 1))
                .build();

        return elasticsearch.search(query, Map.class, IndexCoordinates.of(target.indexName));
    }

    private Set<String> safeExcluded(Set<String> excludedIds) {
        return excludedIds == null ? Set.of() : new HashSet<>(excludedIds);
    }

    private boolean hasId(String id) {
        return id != null && !id.isBlank();
    }

    private enum Target {
        USERS("user_detail_vector", "user_long_term_vector", "user_long_term_vector_model",
                "user_long_term_vector_dimension", "user_long_term_vector_schema_version"),
        POSTS("post_vector", "content_vector", "model", "dimension", "schema_version");

        private final String indexName;
        private final String vectorField;
        private final String modelField;
        private final String dimensionField;
        private final String schemaVersionField;

        Target(String indexName, String vectorField, String modelField, String dimensionField, String schemaVersionField) {
            this.indexName = indexName;
            this.vectorField = vectorField;
            this.modelField = modelField;
            this.dimensionField = dimensionField;
            this.schemaVersionField = schemaVersionField;
        }
    }
}
