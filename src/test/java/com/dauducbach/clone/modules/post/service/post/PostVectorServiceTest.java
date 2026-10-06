package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.modules.post.entity.*;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.modules.post.repositoty.*;
import com.dauducbach.clone.modules.user.dto.UserVectorSnapshot;
import com.dauducbach.clone.modules.user.service.UserVectorSnapshotService;
import com.dauducbach.clone.utils.GetVectorEmbedding;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class PostVectorServiceTest {
    final PostDetailsRepository posts = mock(PostDetailsRepository.class);
    final PostItemRepository items = mock(PostItemRepository.class);
    final GetVectorEmbedding embeddings = mock(GetVectorEmbedding.class);
    final PostVectorStore store = mock(PostVectorStore.class);
    final UserVectorSnapshotService snapshots = mock(UserVectorSnapshotService.class);
    final AtomicReference<PostDetails> current = new AtomicReference<>(post("text"));
    final AtomicReference<List<PostItem>> media = new AtomicReference<>(List.of());
    final AtomicReference<PostVector> saved = new AtomicReference<>();
    final PostEmbeddingTextBuilder text = new PostEmbeddingTextBuilder();
    final PostVectorService service;
    PostVectorServiceTest() {
        when(posts.findById("p")).thenAnswer(i -> Mono.defer(() -> Mono.justOrEmpty(current.get())));
        when(items.findByPostIdOrderByOrderNumberAsc("p")).thenAnswer(i -> Flux.defer(() -> Flux.fromIterable(media.get())));
        when(store.load("p")).thenAnswer(i -> Mono.defer(() -> Mono.justOrEmpty(saved.get())));
        when(store.commit(any(), any())).thenAnswer(i -> Mono.fromRunnable(() -> saved.set(i.getArgument(0))));
        when(store.retainDeletionTombstone(anyString(), any())).thenReturn(Mono.empty());
        when(embeddings.getEmbedding(anyString())).thenReturn(Mono.just(axis(0)));
        when(snapshots.load("a")).thenReturn(Mono.just(snapshot(List.of(), axis(1))));
        service = new PostVectorService(embeddings, store, new PostEmbeddingSourceReader(posts, items, text), snapshots);
    }
    @Test void mixesContentAndHistoricalAuthorAndPersistsSeparateRawSearchVector() {
        service.rebuild("p").block();
        assertThat(saved.get()).isNotNull();
        assertThat(saved.get().getContentVector()).isEqualTo(axis(0));
        assertThat(saved.get().getRecommendationVector()).isEqualTo(VectorMath.mix(axis(0), .7, axis(1), .3));
        assertThat(saved.get().getAuthorVectorVersion()).isEqualTo(7);
        assertThat(saved.get().getAuthorId()).isEqualTo("a");
        assertThat(saved.get().getEmbeddingState()).isEqualTo("READY");
        assertThat(saved.get().getSourceRevision()).isEqualTo(text.revision(current.get(), media.get()));
    }
    @Test void usesProfileWhenLongTermMissing() {
        when(snapshots.load("a")).thenReturn(Mono.just(snapshot(axis(2), List.of())));
        service.rebuild("p").block();
        assertThat(saved.get()).isNotNull();
        assertThat(saved.get().getRecommendationVector()).isEqualTo(VectorMath.mix(axis(0), .7, axis(2), .3));
    }
    @Test void missingAuthorUsesContentOnly() {
        when(snapshots.load("a")).thenReturn(Mono.empty()); service.rebuild("p").block();
        assertThat(saved.get()).isNotNull(); assertThat(saved.get().getRecommendationVector()).isEqualTo(axis(0));
    }
    @Test void emptyTextUsesAuthorOnlyWithoutProvider() {
        current.set(post(" ")); service.rebuild("p").block();
        assertThat(saved.get()).isNotNull(); assertThat(saved.get().getRecommendationVector()).isEqualTo(axis(1));
        assertThat(saved.get().getContentVector()).isNullOrEmpty(); verifyNoInteractions(embeddings);
    }
    @Test void noInputPersistsCurrentSkipState() {
        current.set(post(" ")); when(snapshots.load("a")).thenReturn(Mono.empty()); service.rebuild("p").block();
        assertThat(saved.get()).isNotNull(); assertThat(saved.get().getEmbeddingState()).isEqualTo("SKIPPED_NO_INPUT");
        assertThat(saved.get().getRecommendationVector()).isNullOrEmpty(); verifyNoInteractions(embeddings);
    }
    @Test void embeddingFailureCannotBeAcknowledged() {
        RuntimeException failure = new RuntimeException("provider unavailable");
        when(embeddings.getEmbedding("text")).thenReturn(Mono.error(failure));
        StepVerifier.create(service.processPostEmbedding("p", "untrusted payload")).expectErrorMatches(e -> e == failure).verify();
        verify(store, never()).commit(any(), any());
    }
    @Test void authorInfrastructureFailureCannotBeTreatedAsMissing() {
        RuntimeException failure = new RuntimeException("ES unavailable"); when(snapshots.load("a")).thenReturn(Mono.error(failure));
        StepVerifier.create(service.rebuild("p")).expectErrorMatches(e -> e == failure).verify(); verifyNoInteractions(embeddings);
    }
    @Test void commitRetryReusesComputedRawEmbeddingByFingerprint() {
        RuntimeException failure = new RuntimeException("ES write failed");
        when(store.commit(any(), any())).thenReturn(Mono.error(failure)).thenAnswer(i -> Mono.fromRunnable(() -> saved.set(i.getArgument(0))));
        StepVerifier.create(service.rebuild("p")).expectErrorMatches(e -> e == failure).verify(); service.rebuild("p").block();
        verify(embeddings, times(1)).getEmbedding("text"); assertThat(saved.get()).isNotNull();
    }
    @Test void fingerprintCacheReusesRawVectorAndStillUpdatesRevisionAndAuthorSnapshot() {
        PostVector prior = ready(); saved.set(prior);
        current.get().setMediaRatio("4:3"); when(snapshots.load("a")).thenReturn(Mono.just(snapshot(List.of(), axis(2))));
        service.rebuild("p").block();
        verifyNoInteractions(embeddings); assertThat(saved.get().getSourceRevision()).isNotEqualTo(prior.getSourceRevision());
        assertThat(saved.get().getRecommendationVector()).isEqualTo(VectorMath.mix(axis(0), .7, axis(2), .3));
        verify(store).commit(any(), same(prior));
    }
    @Test void unchangedReadySourceDoesNotCascadeAuthorChanges() {
        saved.set(ready()); service.rebuild("p").block();
        verifyNoInteractions(embeddings, snapshots); verify(store, never()).commit(any(), any());
    }
    @Test void changedSourceDuringEmbeddingDiscardsOldComputation() {
        Sinks.One<List<Double>> pending = Sinks.one(); when(embeddings.getEmbedding("text")).thenReturn(pending.asMono());
        var future = service.rebuild("p").toFuture(); current.set(post("new text")); pending.tryEmitValue(axis(0));
        assertThatThrownBy(future::join).hasCauseInstanceOf(PostVectorPendingException.class);
        verify(store, never()).commit(any(), any());
    }
    @Test void deletedSourceDuringEmbeddingCreatesFenceRatherThanResurrection() {
        Sinks.One<List<Double>> pending = Sinks.one(); when(embeddings.getEmbedding("text")).thenReturn(pending.asMono());
        var future = service.rebuild("p").toFuture(); current.set(null); pending.tryEmitValue(axis(0)); future.join();
        verify(store).retainDeletionTombstone("p", "a"); verify(store, never()).commit(any(), any());
    }
    @Test void existingBaselineIsCapturedBeforeEmbeddingAndNeverRefetchedAtCommit() {
        PostVector prior = ready(); prior.setContentFingerprint("older"); saved.set(prior);
        Sinks.One<List<Double>> pending = Sinks.one(); when(embeddings.getEmbedding("text")).thenReturn(pending.asMono());
        var future = service.rebuild("p").toFuture(); saved.set(ready()); pending.tryEmitValue(axis(0)); future.join();
        verify(store, times(1)).load("p"); verify(store).commit(any(), same(prior));
    }
    @Test void pendingModerationIsRetryableAndNeverEmbedded() {
        current.get().setValidateStatus("PENDING_SCAN");
        StepVerifier.create(service.rebuild("p")).expectError(PostVectorPendingException.class).verify(); verifyNoInteractions(embeddings);
    }
    @Test void rejectedRevisionCanLaterBeApprovedWithoutPermanentDeletionPoisoning() {
        current.get().setValidateStatus("REJECTED"); service.rebuild("p").block();
        assertThat(saved.get()).isNotNull(); assertThat(saved.get().getEmbeddingState()).isEqualTo("REJECTED");
        assertThat(saved.get().getDeleted()).isFalse(); assertThat(saved.get().getContentVector()).isNullOrEmpty();
        current.set(post("approved edit")); service.rebuild("p").block();
        assertThat(saved.get().getEmbeddingState()).isEqualTo("READY"); verify(embeddings).getEmbedding("approved edit");
        verify(store, never()).retainDeletionTombstone(anyString(), any());
    }
    @Test void unknownModelRawVectorIsReembeddedRatherThanStamped() {
        PostVector old = ready(); old.setModel(null); saved.set(old); service.rebuild("p").block();
        verify(embeddings).getEmbedding("text"); assertThat(saved.get().getModel()).isEqualTo(VectorMath.MODEL);
    }
    @Test void bulkCleanupUnionsSqlIdsWithPersistedAuthorIdsForRetryAfterSqlDeletion() {
        when(store.findPostIdsByAuthor("a")).thenReturn(Flux.just("orphan", "p"));
        service.deletePostsByAuthor("a", List.of("p", "sql-only")).block();
        verify(store).retainDeletionTombstone("orphan", "a"); verify(store).retainDeletionTombstone("p", "a");
        verify(store).retainDeletionTombstone("sql-only", "a");
    }
    static PostDetails post(String content) { return PostDetails.builder().postId("p").userId("a").validateStatus("APPROVED").content(content).build(); }
    static List<Double> axis(int index) { List<Double> vector = new ArrayList<>(Collections.nCopies(768, 0.0)); vector.set(index, 1.0); return vector; }
    static UserVectorSnapshot snapshot(List<Double> profile, List<Double> longTerm) { return new UserVectorSnapshot(7, profile, longTerm, List.of(), true, VectorMath.MODEL); }
    PostVector ready() { return PostVector.builder().postId("p").authorId("a").model(VectorMath.MODEL).dimension(768).schemaVersion(1)
            .contentVector(axis(0)).recommendationVector(axis(1)).contentFingerprint(text.fingerprint(current.get(), media.get()))
            .sourceRevision(text.revision(current.get(), media.get())).embeddingState("READY").seqNoPrimaryTerm(new SeqNoPrimaryTerm(3_000_000_000L, 2)).build(); }
}
