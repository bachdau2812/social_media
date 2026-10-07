package com.dauducbach.clone.modules.personalization.snapshots;
import org.junit.jupiter.api.Test;
import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.modules.personalization.snapshots.PreferenceSnapshotService;
import com.dauducbach.clone.modules.personalization.support.VectorOperationFixture;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import reactor.test.StepVerifier;
import static org.mockito.Mockito.mock;
import reactor.core.publisher.Mono;
import static org.mockito.Mockito.when;
import static org.assertj.core.api.Assertions.*;
class PreferenceSnapshotServiceTest {
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

    @Test void adaptsThePreferenceSnapshotToThePostOwnedConsumerContract() {
        var f = new VectorOperationFixture();
        f.profile("first", 0).block();
        var context = service(f).loadPostAuthorContext("u").block();

        assertThat(context.version()).isEqualTo(1);
        assertThat(context.profile()).isEqualTo(VectorOperationFixture.vector(0));
        assertThat(context.longTerm()).isEqualTo(VectorOperationFixture.vector(0));
        assertThat(context.model()).isEqualTo(VectorMath.MODEL);
    }
    @Test void feedQueueLeaseKeepsTheSharedRedisFenceWhileHidingVectorAdapters() {
        var f = new VectorOperationFixture();
        var service = service(f);
        var version = service.withSnapshotLease("u", lease -> {
            assertThat(lease.userId()).isEqualTo("u");
            assertThat(lease.ownershipToken()).isNotBlank();
            assertThat(lease.lockKey()).isEqualTo("vector:lock:u");
            assertThat(lease.versionKey()).isEqualTo("user_vector_version:u");
            assertThat(lease.dirtyKey()).isEqualTo("feed:dirty:u");
            return service.load(lease).map(snapshot -> snapshot.version());
        }).block();
        assertThat(version).isZero();
    }
    private PreferenceSnapshotService service(VectorOperationFixture f) {
        return new PreferenceSnapshotService(f.coordinator, f.recovery, f.query, f.users, f.redis);
    }
}
