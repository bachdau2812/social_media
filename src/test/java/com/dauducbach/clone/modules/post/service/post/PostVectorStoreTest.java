package com.dauducbach.clone.modules.post.service.post;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch._types.*;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.Map;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;
@SuppressWarnings({"unchecked", "rawtypes"})
class PostVectorStoreTest {
    final ReactiveElasticsearchClient client = mock(ReactiveElasticsearchClient.class);
    final ReactiveElasticsearchOperations operations = mock(ReactiveElasticsearchOperations.class);
    final PostVectorStore store = new PostVectorStore(client, operations);
    @Test void existingUsesCapturedLongOccWithoutConflictRetryOrUpsertAndRejectsTombstones() {
        when(client.update(any(UpdateRequest.class), eq(Map.class))).thenReturn(Mono.empty());
        PostVector old = PostVector.builder().postId("p").seqNoPrimaryTerm(new SeqNoPrimaryTerm(3_000_000_000L, 2)).build();
        store.commit(ready(), old).block();
        ArgumentCaptor<UpdateRequest> capture = ArgumentCaptor.forClass(UpdateRequest.class); verify(client).update(capture.capture(), eq(Map.class));
        assertThat(capture.getValue().ifSeqNo()).isEqualTo(3_000_000_000L); assertThat(capture.getValue().ifPrimaryTerm()).isEqualTo(2);
        assertThat(capture.getValue().upsert()).isNull(); assertThat(capture.getValue().retryOnConflict()).isNull();
        assertThat(capture.getValue().script().source()).contains("deleted == true", "throw new IllegalStateException");
    }
    @Test void absentBaselineCreatesOnlyAndTombstoneRetainsAuthorWithNoSearchableVectors() {
        when(client.index(any(IndexRequest.class))).thenReturn(Mono.empty());
        store.commit(ready(), PostVector.builder().build()).block(); store.retainDeletionTombstone("p", "a").block();
        ArgumentCaptor<IndexRequest> capture = ArgumentCaptor.forClass(IndexRequest.class); verify(client, times(2)).index(capture.capture());
        assertThat(capture.getAllValues().get(0).opType()).isEqualTo(OpType.Create);
        assertThat((Map) capture.getAllValues().get(0).document()).containsKeys("content_vector", "recommendation_vector", "source_revision", "author_id");
        assertThat(capture.getAllValues().get(1).opType()).isEqualTo(OpType.Index);
        assertThat((Map) capture.getAllValues().get(1).document()).containsExactlyInAnyOrderEntriesOf(Map.of("post_id", "p", "author_id", "a", "deleted", true));
    }
    @Test void missingTokensOnExistingDocumentAndMalformedVectorNeverReachEs() {
        StepVerifier.create(store.commit(ready(), PostVector.builder().postId("p").build())).expectError().verify();
        PostVector malformed = ready(); malformed.setContentVector(java.util.List.of(1.0));
        StepVerifier.create(store.commit(malformed, PostVector.builder().build())).expectError().verify(); verifyNoInteractions(client);
    }
    @Test void conflictAndInfrastructureErrorsPropagateWithoutRetry() {
        RuntimeException failure = new ElasticsearchException("index", new ErrorResponse.Builder().status(409).error(e -> e.type("version_conflict_engine_exception").reason("stale")).build());
        when(client.index(any(IndexRequest.class))).thenReturn(Mono.error(failure));
        StepVerifier.create(store.commit(ready(), PostVector.builder().build())).expectError().verify(); verify(client).index(any(IndexRequest.class));
    }
    PostVector ready() { return PostVector.builder().postId("p").authorId("a").model("gemini-embedding-2").dimension(768).schemaVersion(1)
            .embeddingState("READY").contentVector(PostVectorServiceTest.axis(0)).recommendationVector(PostVectorServiceTest.axis(1))
            .sourceRevision("r").contentFingerprint("f").authorVectorVersion(7L).build(); }
}
