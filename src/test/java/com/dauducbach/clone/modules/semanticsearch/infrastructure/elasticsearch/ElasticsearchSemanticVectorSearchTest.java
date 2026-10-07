package com.dauducbach.clone.modules.semanticsearch.infrastructure.elasticsearch;

import com.dauducbach.clone.modules.embedding.publicapi.TextEmbeddingProvider;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.client.elc.NativeQuery;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ElasticsearchSemanticVectorSearchTest {
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void textUserSearchUsesTheEmbeddingPortAndReturnsOnlyAllowedIds() {
        TextEmbeddingProvider embeddings = mock(TextEmbeddingProvider.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        when(embeddings.getEmbedding("query")).thenReturn(Mono.just(vector()));
        SearchHit<Map> excluded = hit("excluded");
        SearchHit<Map> first = hit("user-1");
        SearchHit<Map> second = hit("user-2");
        doReturn(Flux.just(excluded, first, second)).when(elasticsearch)
                .search(any(Query.class), eq(Map.class), any(IndexCoordinates.class));
        var search = new ElasticsearchSemanticVectorSearch(embeddings, elasticsearch);

        StepVerifier.create(search.searchUserIds("query", 1, Set.of("excluded")))
                .expectNext(List.of("user-1"))
                .verifyComplete();

        verify(embeddings).getEmbedding("query");
        var index = org.mockito.ArgumentCaptor.forClass(IndexCoordinates.class);
        verify(elasticsearch).search(any(Query.class), eq(Map.class), index.capture());
        assertThat(index.getValue().getIndexNames()).containsExactly("user_detail_vector");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void vectorUserSearchDoesNotCallEmbeddingProvider() {
        TextEmbeddingProvider embeddings = mock(TextEmbeddingProvider.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        SearchHit<Map> first = hit("user-1");
        doReturn(Flux.just(first)).when(elasticsearch)
                .search(any(Query.class), eq(Map.class), any(IndexCoordinates.class));
        var search = new ElasticsearchSemanticVectorSearch(embeddings, elasticsearch);

        StepVerifier.create(search.searchUserIdsByVector(vector(), 5, Set.of()))
                .expectNext(List.of("user-1"))
                .verifyComplete();

        verifyNoInteractions(embeddings);
        var query = org.mockito.ArgumentCaptor.forClass(Query.class);
        verify(elasticsearch).search(query.capture(), eq(Map.class), any(IndexCoordinates.class));
        NativeQuery nativeQuery = (NativeQuery) query.getValue();
        assertThat(nativeQuery.getMinScore()).isEqualTo(1.50f);
        assertThat(nativeQuery.getQuery().scriptScore().minScore()).isEqualTo(1.50f);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void textSearchRetainsItsExistingThresholdForUsersAndPosts() {
        TextEmbeddingProvider embeddings = mock(TextEmbeddingProvider.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        when(embeddings.getEmbedding("query")).thenReturn(Mono.just(vector()));
        doReturn(Flux.empty()).when(elasticsearch)
                .search(any(Query.class), eq(Map.class), any(IndexCoordinates.class));
        var search = new ElasticsearchSemanticVectorSearch(embeddings, elasticsearch);

        StepVerifier.create(search.searchUserIds("query", 5, Set.of())).expectNext(List.of()).verifyComplete();
        StepVerifier.create(search.searchPostIds("query", 5, Set.of())).expectNext(List.of()).verifyComplete();

        var query = org.mockito.ArgumentCaptor.forClass(Query.class);
        verify(elasticsearch, times(2)).search(query.capture(), eq(Map.class), any(IndexCoordinates.class));
        assertThat(query.getAllValues()).allSatisfy(value -> {
            NativeQuery nativeQuery = (NativeQuery) value;
            assertThat(nativeQuery.getMinScore()).isEqualTo(0.80f + 1.0f);
            assertThat(nativeQuery.getQuery().scriptScore().minScore()).isEqualTo(0.80f + 1.0f);
        });
    }

    private static List<Double> vector() {
        var values = new java.util.ArrayList<>(Collections.nCopies(768, 0.0));
        values.set(0, 1.0);
        return values;
    }

    @SuppressWarnings("unchecked")
    private static SearchHit<Map> hit(String id) {
        SearchHit<Map> hit = mock(SearchHit.class);
        when(hit.getId()).thenReturn(id);
        return hit;
    }
}
