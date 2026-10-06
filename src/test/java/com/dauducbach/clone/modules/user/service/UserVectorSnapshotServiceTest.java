package com.dauducbach.clone.modules.user.service;
import org.junit.jupiter.api.Test;
import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.utils.GsonUtils;
import reactor.test.StepVerifier;
import com.dauducbach.clone.modules.feed.service.FeedInteractionProcessingService;
import org.springframework.beans.factory.ObjectProvider;
import static org.mockito.Mockito.mock;
import reactor.core.publisher.Mono;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.*;
class UserVectorSnapshotServiceTest {
    @Test void reconcilesPendingBeforeReturningThreeImmutableVectorsAndCommittedVersion() {
        var f = new VectorOperationFixture(); f.redis.failCommit = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify(); f.redis.failCommit = false;
        f.redis.shortJson = GsonUtils.getGson().toJson(VectorOperationFixture.vector(1)); f.redis.shortModel = VectorMath.MODEL;
        var result = service(f).load("u").block();
        assertThat(result.version()).isEqualTo(1); assertThat(result.profile()).isEqualTo(VectorOperationFixture.vector(0));
        assertThat(result.longTerm()).isEqualTo(VectorOperationFixture.vector(0));
        assertThat(result.shortTerm()).isEqualTo(VectorOperationFixture.vector(1));
        assertThat(result.model()).isEqualTo(VectorMath.MODEL); assertThat(result.hasLearnedHistory()).isFalse();
        assertThatThrownBy(() -> result.profile().set(0, 99d)).isInstanceOf(UnsupportedOperationException.class);
        f.document.getUserVector().set(0, 55d); assertThat(result.profile().getFirst()).isEqualTo(1d);
    }
    @Test void validatesProfileAndLongTermModelsIndependentlyAndSkipsUnknownLegacyShortTerm() {
        var f = new VectorOperationFixture(); f.profile("first", 0).block();
        f.document.setUserVectorModel("other-model");
        f.redis.shortJson = GsonUtils.getGson().toJson(VectorOperationFixture.vector(1));
        var result = service(f).load("u").block();
        assertThat(result.profile()).isEmpty(); assertThat(result.longTerm()).isNotEmpty(); assertThat(result.shortTerm()).isEmpty();
        f.document.setUserVectorModel(VectorMath.MODEL); f.document.setUserLongTermVectorModel(null);
        result = service(f).load("u").block();
        assertThat(result.profile()).isNotEmpty(); assertThat(result.longTerm()).isEmpty();
    }
    @Test void pendingCommitFailureNeverLeaksPartialSnapshot() {
        var f = new VectorOperationFixture(); f.redis.failCommit = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        StepVerifier.create(service(f).load("u")).expectError().verify();
    }
    @Test void infrastructureErrorPropagatesAndDeletedUserReturnsNoSnapshot() {
        var f = new VectorOperationFixture(); f.userExists = false;
        StepVerifier.create(service(f).load("u")).verifyComplete();
        f.userExists = true; when(f.query.getSnapshot("u")).thenReturn(Mono.error(new IllegalStateException("ES offline")));
        StepVerifier.create(service(f).load("u")).expectErrorMessage("ES offline").verify();
    }
    private UserVectorSnapshotService service(VectorOperationFixture f) {
        FeedInteractionProcessingService processing = mock(FeedInteractionProcessingService.class);
        when(processing.reconcilePending(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());
        when(processing.requireContinuity(org.mockito.ArgumentMatchers.any())).thenReturn(Mono.empty());
        @SuppressWarnings("unchecked")
        ObjectProvider<FeedInteractionProcessingService> provider = mock(ObjectProvider.class);
        when(provider.getObject()).thenReturn(processing);
        return new UserVectorSnapshotService(f.coordinator, f.service, f.query, f.users, f.redis, provider);
    }
}
