package com.dauducbach.clone.infrastructure.vector;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.assertj.core.api.Assertions.*;

class UserVectorCoordinatorTest {
    @Test void renewsDuringLongWorkAndReleasesAfterSuccess() {
        var redis = new InMemoryVectorRedisState(); var coordinator = new UserVectorCoordinator(redis);
        StepVerifier.withVirtualTime(() -> coordinator.withUserLock("u", lease -> Mono.delay(Duration.ofSeconds(16)).thenReturn("done")))
                .thenAwait(Duration.ofSeconds(16)).expectNext("done").verifyComplete();
        assertThat(redis.renewals).isEqualTo(3); assertThat(redis.owner).isNull();
    }
    @Test void lostLeaseCancelsInflightWorkAndCannotDeleteReplacementOwner() {
        var redis = new InMemoryVectorRedisState(); var coordinator = new UserVectorCoordinator(redis);
        var canceled = new AtomicBoolean();
        StepVerifier.withVirtualTime(() -> coordinator.withUserLock("u", lease -> Mono.never().doOnCancel(() -> canceled.set(true))))
                .then(() -> redis.owner = "replacement").thenAwait(Duration.ofSeconds(5))
                .expectError(UserVectorCoordinator.LeaseLostException.class).verify();
        assertThat(canceled).isTrue(); assertThat(redis.owner).isEqualTo("replacement");
    }
    @Test void cancellationAndErrorReleaseOwnedLease() {
        var redis = new InMemoryVectorRedisState(); var coordinator = new UserVectorCoordinator(redis);
        StepVerifier.create(coordinator.withUserLock("u", lease -> Mono.never())).thenCancel().verify();
        assertThat(redis.owner).isNull();
        StepVerifier.create(coordinator.withUserLock("u", lease -> Mono.error(new IllegalStateException("failed"))))
                .expectError(IllegalStateException.class).verify();
        assertThat(redis.owner).isNull();
    }
    @Test void competingReaderTimesOutWithoutEnteringOrReleasingOtherOwner() {
        var redis = new InMemoryVectorRedisState(); var coordinator = new UserVectorCoordinator(redis); redis.owner = "other";
        var entered = new AtomicBoolean();
        StepVerifier.withVirtualTime(() -> coordinator.withSnapshotLock("u", lease -> { entered.set(true); return Mono.just(1); }))
                .thenAwait(Duration.ofSeconds(2)).expectError(java.util.concurrent.TimeoutException.class).verify();
        assertThat(entered).isFalse(); assertThat(redis.owner).isEqualTo("other");
    }
    @Test void emptyWorkAlsoChecksOwnershipBeforeReportingSuccess() {
        var redis = new InMemoryVectorRedisState(); var coordinator = new UserVectorCoordinator(redis);
        StepVerifier.create(coordinator.withUserLock("u", lease -> Mono.fromRunnable(() -> redis.owner = "replacement")))
                .expectError(UserVectorCoordinator.LeaseLostException.class).verify();
    }
    @Test void commitIsIdempotentAndWrongTokenCannotCommit() {
        var redis = new InMemoryVectorRedisState(); redis.owner = "token";
        var lease = new VectorLease("u", "token");
        assertThat(redis.commit(lease, "operation").block()).isEqualTo(1L);
        assertThat(redis.commit(lease, "operation").block()).isEqualTo(1L);
        StepVerifier.create(redis.commit(new VectorLease("u", "wrong"), "new"))
                .expectError(UserVectorCoordinator.LeaseLostException.class).verify();
        assertThat(redis.version).isEqualTo(1);
    }
}
