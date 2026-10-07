package com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.ErrorResponse;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class UserVectorStoreTest {
    private final ReactiveElasticsearchClient client = mock(ReactiveElasticsearchClient.class);

    @Test @SuppressWarnings({"unchecked", "rawtypes"})
    void fencedProfileUsesCapturedOccAndCreateOnlyForMissingDocument() {
        when(client.update(any(UpdateRequest.class), eq(Map.class))).thenReturn(Mono.empty());
        when(client.index(any(co.elastic.clients.elasticsearch.core.IndexRequest.class))).thenReturn(Mono.empty());
        store().upsertProfileAndSeedLongTerm("user", oneHot(), "existing", 3_000_000_000L, 2L).block();
        ArgumentCaptor<UpdateRequest> update = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(client).update(update.capture(), eq(Map.class));
        assertThat(update.getValue().ifSeqNo()).isEqualTo(3_000_000_000L);
        assertThat(update.getValue().ifPrimaryTerm()).isEqualTo(2L);
        assertThat(update.getValue().retryOnConflict()).isNull(); assertThat(update.getValue().upsert()).isNull();
        assertThat(update.getValue().script().source()).contains("ctx._source.deleted == true", "throw new IllegalStateException");
        store().upsertProfileAndSeedLongTerm("user", oneHot(), "new", null, null).block();
        ArgumentCaptor<co.elastic.clients.elasticsearch.core.IndexRequest> create = ArgumentCaptor.forClass(co.elastic.clients.elasticsearch.core.IndexRequest.class);
        verify(client).index(create.capture());
        assertThat(create.getValue().opType()).isEqualTo(co.elastic.clients.elasticsearch._types.OpType.Create);
        assertThat((Map) create.getValue().document()).containsEntry("user_profile_operation_id", "new")
                .containsEntry("has_learned_history", false).containsEntry("user_long_term_vector_model", "gemini-embedding-2");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void createReplayUsesSameAtomicConditionalSeedScriptAfterLongTermWriter() {
        when(client.update(any(UpdateRequest.class), eq(Map.class))).thenReturn(Mono.empty());
        UserVectorStore store = store();
        writeProfile(store, "create-1").block();
        writeLongTerm(store, "learn-1", 9, 2).block();
        writeProfile(store, "create-1").block();
        ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(client, times(3)).update(captor.capture(), eq(Map.class));
        UpdateRequest profile = captor.getAllValues().get(0);
        assertThat(profile.index()).isEqualTo("user_detail_vector");
        assertThat(profile.id()).isEqualTo("user");
        assertThat(profile.doc()).isNull();
        assertThat(profile.scriptedUpsert()).isTrue();
        assertThat((Map) profile.upsert()).containsEntry("user_id", "user");
        assertThat(profile.retryOnConflict()).isEqualTo(3);
        String script = profile.script().source();
        assertThat(script).contains("ctx._source.user_vector = params.vector", "ctx._source.user_long_term_vector == null", "ctx._source.user_long_term_vector.isEmpty()", "ctx._source.has_learned_history = false");
        assertThat(script.indexOf("ctx._source.has_learned_history = false"))
                .isGreaterThan(script.indexOf("ctx._source.user_long_term_vector.isEmpty()"));
        assertThat(script).doesNotContain("ctx._source.has_learned_history = true");
        assertThat(profile.script().params().get("vector").to(List.class)).hasSize(768);
        assertThat(profile.script().params().get("model").to(String.class)).isEqualTo("gemini-embedding-2");
        assertThat(profile.script().params().get("dimension").to(Integer.class)).isEqualTo(768);
        assertThat(profile.script().params().get("schemaVersion").to(Integer.class)).isEqualTo(1);
        assertThat(captor.getAllValues().get(2).script().source()).isEqualTo(script);
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void longTermOnlyWritesItsFieldsAndKeepsLongOptimisticConcurrencyTokens() {
        when(client.update(any(UpdateRequest.class), eq(Map.class))).thenReturn(Mono.empty());
        writeLongTerm(store(), "learn-2", 3_000_000_000L, 4_000_000_000L).block();
        ArgumentCaptor<UpdateRequest> captor = ArgumentCaptor.forClass(UpdateRequest.class);
        verify(client).update(captor.capture(), eq(Map.class));
        UpdateRequest request = captor.getValue();
        assertThat(request.ifSeqNo()).isEqualTo(3_000_000_000L);
        assertThat(request.ifPrimaryTerm()).isEqualTo(4_000_000_000L);
        assertThat(request.upsert()).isNull();
        assertThat(request.retryOnConflict()).isNull();
        assertThat(request.script().source()).contains("ctx._source.deleted == true", "throw new IllegalStateException", "ctx._source.user_long_term_vector = params.vector", "ctx._source.has_learned_history = true", "ctx._source.long_term_version");
        assertThat(request.script().source()).doesNotContain("ctx._source.user_vector =", "ctx._source.profile_version =");
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void versionConflictSignalsFreshReadAndRecomputeInsteadOfSilentRetry() {
        ElasticsearchException conflict = new ElasticsearchException("update", new ErrorResponse.Builder().status(409)
                .error(error -> error.type("version_conflict_engine_exception").reason("stale seqNo")).build());
        when(client.update(any(UpdateRequest.class), eq(Map.class))).thenReturn(Mono.error(conflict));
        StepVerifier.create(writeLongTerm(store(), "learn-3", 1, 1))
                .expectErrorMatches(error -> error.getClass().getSimpleName().equals("RetryableVectorConflictException") && error.getCause() == conflict).verify();
        verify(client).update(any(UpdateRequest.class), eq(Map.class));
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void infrastructureFailuresPropagateFromBothWrites() {
        IllegalStateException failure = new IllegalStateException("ES offline");
        when(client.update(any(UpdateRequest.class), eq(Map.class))).thenReturn(Mono.error(failure));
        UserVectorStore store = store();
        StepVerifier.create(writeProfile(store, "profile-1")).expectErrorMatches(error -> error == failure).verify();
        StepVerifier.create(writeLongTerm(store, "learn-1", 1, 1)).expectErrorMatches(error -> error == failure).verify();
    }

    @Test
    void rejectsMalformedVectorWithoutCallingElasticsearch() {
        StepVerifier.create(store().upsertProfileAndSeedLongTerm("user", List.of(1.0), "profile-1"))
                .expectError(IllegalArgumentException.class).verify();
        StepVerifier.create(store().updateLongTerm("user", Collections.nCopies(768, 0.0), "learn-1", 1, 1))
                .expectError(IllegalArgumentException.class).verify();
        verifyNoInteractions(client);
    }

    @Test
    void rejectsMissingIdentityOrInvalidConcurrencyTokensWithoutCallingElasticsearch() {
        StepVerifier.create(store().upsertProfileAndSeedLongTerm(" ", oneHot(), "profile-1"))
                .expectError(IllegalArgumentException.class).verify();
        StepVerifier.create(store().upsertProfileAndSeedLongTerm("user", oneHot(), null))
                .expectError(IllegalArgumentException.class).verify();
        StepVerifier.create(store().updateLongTerm("user", oneHot(), "learn-1", -1, 1))
                .expectError(IllegalArgumentException.class).verify();
        StepVerifier.create(store().updateLongTerm("user", oneHot(), "learn-1", 1, 0))
                .expectError(IllegalArgumentException.class).verify();
        verifyNoInteractions(client);
    }


    @Test @SuppressWarnings({"unchecked", "rawtypes"})
    void deletionReplacesEntireSourceWithVectorFreeTombstone() {
        when(client.index(any(co.elastic.clients.elasticsearch.core.IndexRequest.class))).thenReturn(Mono.empty());
        store().retainDeletionTombstone("user").block();
        ArgumentCaptor<co.elastic.clients.elasticsearch.core.IndexRequest> request = ArgumentCaptor.forClass(co.elastic.clients.elasticsearch.core.IndexRequest.class);
        verify(client).index(request.capture());
        assertThat((Map) request.getValue().document()).containsExactlyInAnyOrderEntriesOf(Map.of("user_id", "user", "deleted", true));
        assertThat(request.getValue().id()).isEqualTo("user");
        assertThat(request.getValue().opType()).isEqualTo(co.elastic.clients.elasticsearch._types.OpType.Index);
    }

    private Mono<Void> writeProfile(UserVectorStore store, String operationId) {
        return store.upsertProfileAndSeedLongTerm("user", oneHot(), operationId);
    }

    private Mono<Void> writeLongTerm(UserVectorStore store, String operationId, long seqNo, long primaryTerm) {
        return store.updateLongTerm("user", oneHot(), operationId, seqNo, primaryTerm);
    }

    private UserVectorStore store() {
        return new UserVectorStore(client);
    }

    private List<Double> oneHot() {
        List<Double> vector = new ArrayList<>(Collections.nCopies(768, 0.0));
        vector.set(0, 3.0);
        return vector;
    }
}
