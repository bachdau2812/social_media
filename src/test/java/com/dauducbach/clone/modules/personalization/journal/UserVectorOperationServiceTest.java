package com.dauducbach.clone.modules.personalization.journal;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.model.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.support.VectorOperationFixture;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import org.springframework.dao.OptimisticLockingFailureException;
import java.util.List;
import static org.assertj.core.api.Assertions.*;
class UserVectorOperationServiceTest {
    @Test void retainedEsAppliedCopyCannotRepublishAfterRecoveryAndNewCommit() {
        var f = new VectorOperationFixture(); f.redis.failCommit = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        var retained = f.copy(f.journal.get("first"));
        assertThat(retained.getStatus()).isEqualTo("ES_APPLIED");
        f.redis.failCommit = false;
        f.coordinator.withUserLock("u", f.service::reconcilePending).block();
        var second = f.profile("second", 1).block();
        var completed = f.coordinator.withUserLock("u", lease -> f.service.complete(lease, retained)).block();
        assertThat(completed.getStatus()).isEqualTo("COMPLETED");
        assertThat(completed.getRowVersion()).isEqualTo(f.journal.get("first").getRowVersion());
        assertThat(f.redis.version).isEqualTo(2);
        assertThat(f.redis.committed).isEqualTo(second.getOperationId());
    }
    @Test void supersededAttemptCopyCannotCompleteRepreparedLogicalKey() {
        var f = new VectorOperationFixture(); f.profile("first", 0).block();
        var captured = VectorOperationFixture.copyDoc(f.document);
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "daily", "LONG_TERM", captured,
                VectorOperationFixture.vector(1), "v1", null, null)).block();
        f.document.setSeqNoPrimaryTerm(new SeqNoPrimaryTerm(4, 1));
        StepVerifier.create(f.coordinator.withUserLock("u", f.service::reconcilePending))
                .expectError(UserVectorStore.RetryableVectorConflictException.class).verify();
        var retained = f.copy(f.journal.get("daily"));
        var fresh = VectorOperationFixture.copyDoc(f.document);
        var completed = f.coordinator.withUserLock("u", lease -> f.service.applyLongTerm(lease, "daily", fresh,
                VectorOperationFixture.vector(2), "v1", null, null)).block();
        StepVerifier.create(f.coordinator.withUserLock("u", lease -> f.service.complete(lease, retained)))
                .expectError(OptimisticLockingFailureException.class).verify();
        assertThat(f.redis.version).isEqualTo(2);
        assertThat(f.redis.committed).isEqualTo(completed.getOperationId());
        assertThat(f.journal.get("daily").getOperationId()).isEqualTo(completed.getOperationId());
    }
    @Test void staleRowVersionIsRejectedBeforeRedisPublication() {
        var f = new VectorOperationFixture(); f.redis.failCommit = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        var retained = f.copy(f.journal.get("first"));
        f.coordinator.withUserLock("u", lease -> f.service.markEsApplied(lease, f.copy(retained))).block();
        f.redis.failCommit = false;
        StepVerifier.create(f.coordinator.withUserLock("u", lease -> f.service.complete(lease, retained)))
                .expectError(OptimisticLockingFailureException.class).verify();
        assertThat(f.redis.version).isZero(); assertThat(f.redis.committed).isNull();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("ES_APPLIED");
    }
    @Test void missingDurableRowIsRejectedBeforeRedisPublication() {
        var f = new VectorOperationFixture(); f.redis.failCommit = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        var retained = f.copy(f.journal.remove("first")); f.redis.failCommit = false;
        StepVerifier.create(f.coordinator.withUserLock("u", lease -> f.service.complete(lease, retained)))
                .expectError(OptimisticLockingFailureException.class).verify();
        assertThat(f.redis.version).isZero(); assertThat(f.redis.committed).isNull();
    }
    @Test void publicCompleteCannotRepublishOldCompletedOperationAfterNewCommit() {
        var f = new VectorOperationFixture(); var first = f.profile("first", 0).block(); f.profile("second", 1).block();
        f.coordinator.withUserLock("u", lease -> f.service.complete(lease, first)).block();
        assertThat(f.redis.version).isEqualTo(2);
    }
    @Test void publicCompleteCannotPublishUnappliedOperation() {
        var f = new VectorOperationFixture();
        StepVerifier.create(f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "first", "PROFILE",
                        new UserDetailVector(), VectorOperationFixture.vector(0), "v1", null, null)
                .flatMap(operation -> f.service.complete(lease, operation))))
                .expectError(IllegalStateException.class).verify();
        assertThat(f.redis.version).isZero();
    }
    @Test void journalRowsHaveOptimisticVersionAgainstDelayedSqlAfterLeaseLoss() throws Exception {
        assertThat(UserVectorUpdateOperation.class.getDeclaredFields())
                .anyMatch(field -> field.isAnnotationPresent(org.springframework.data.annotation.Version.class));
    }
    @Test void recoveryRecognizesEsOperationEvenWhenSqlNeverRecordedEsApplied() {
        var f = new VectorOperationFixture(); f.failApplied = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("PREPARED");
        f.failApplied = false; f.coordinator.withUserLock("u", f.service::reconcilePending).block();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("COMPLETED");
        assertThat(f.esWrites).isEqualTo(1); assertThat(f.redis.version).isEqualTo(1);
    }
    @Test void leaseLossDuringEsCancelsAndNextOwnerReconcilesAmbiguousWrite() {
        var f = new VectorOperationFixture(); f.hangAfterEs = true;
        StepVerifier.withVirtualTime(() -> f.profile("first", 0))
                .then(() -> f.redis.owner = "replacement").thenAwait(java.time.Duration.ofSeconds(5))
                .expectError(com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator.LeaseLostException.class).verify();
        assertThat(f.redis.owner).isEqualTo("replacement"); assertThat(f.redis.version).isZero();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("PREPARED");
        f.redis.owner = null; f.hangAfterEs = false;
        f.coordinator.withUserLock("u", f.service::reconcilePending).block();
        assertThat(f.esWrites).isEqualTo(1); assertThat(f.redis.version).isEqualTo(1);
    }
    @Test void seedLearnProfileRetryPreservesLearnedVectorAndDeduplicatesOperationKey() {
        var f = new VectorOperationFixture(); f.profile("create", 0).block();
        UserDetailVector captured = VectorOperationFixture.copyDoc(f.document);
        f.coordinator.withUserLock("u", lease -> f.service.applyLongTerm(lease, "daily", captured,
                VectorOperationFixture.vector(1), "v1", null, null)).block();
        f.profile("update", 2).block(); f.profile("create", 0).block();
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(2));
        assertThat(f.document.getUserLongTermVector()).isEqualTo(VectorOperationFixture.vector(1));
        assertThat(f.document.getHasLearnedHistory()).isTrue();
        assertThat(f.esWrites).isEqualTo(3); assertThat(f.redis.version).isEqualTo(3);
    }
    @Test void crashAfterEsBeforeRedisIsRecoveredBeforeNextWriter() {
        var f = new VectorOperationFixture(); f.redis.failCommit = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("ES_APPLIED");
        f.redis.failCommit = false; f.profile("second", 1).block();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("COMPLETED");
        assertThat(f.redis.version).isEqualTo(2); assertThat(f.esWrites).isEqualTo(2);
    }
    @Test void crashAfterRedisBeforeSqlCompletionDoesNotIncrementTwice() {
        var f = new VectorOperationFixture(); f.failCompletion = true;
        StepVerifier.create(f.profile("first", 0)).expectError().verify();
        assertThat(f.redis.version).isEqualTo(1); f.failCompletion = false;
        f.coordinator.withUserLock("u", f.service::reconcilePending).block();
        assertThat(f.redis.version).isEqualTo(1); assertThat(f.esWrites).isEqualTo(1);
        assertThat(f.journal.get("first").getStatus()).isEqualTo("COMPLETED");
    }
    @Test void ambiguousEsSuccessUsesOperationIdInsteadOfApplyingAgain() {
        var f = new VectorOperationFixture(); f.failAfterEs = true; f.profile("first", 0).block();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("COMPLETED");
        assertThat(f.esWrites).isEqualTo(1); assertThat(f.redis.version).isEqualTo(1);
    }
    @Test void staleCapturedLongTermDoesNotPrepareOrWrite() {
        var f = new VectorOperationFixture(); f.profile("first", 0).block();
        var stale = VectorOperationFixture.copyDoc(f.document); f.profile("second", 1).block();
        StepVerifier.create(f.coordinator.withUserLock("u", lease -> f.service.applyLongTerm(lease, "daily", stale,
                VectorOperationFixture.vector(2), "v1", null, null)))
                .expectError(UserVectorStore.RetryableVectorConflictException.class).verify();
        assertThat(f.journal).doesNotContainKey("daily"); assertThat(f.esWrites).isEqualTo(2);
    }
    @Test void deletedUserPendingDoesNotRecreateDocument() {
        var f = new VectorOperationFixture();
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "first", "PROFILE", new UserDetailVector(),
                VectorOperationFixture.vector(0), "v1", null, null)).block();
        f.userExists = false;
        StepVerifier.create(f.coordinator.withUserLock("u", f.service::reconcilePending)).expectError().verify();
        assertThat(f.esWrites).isZero(); assertThat(f.document).isNull();
        assertThat(f.journal.get("first").getStatus()).isEqualTo("DELETED");
    }
    @Test void emptyBatchCheckpointDoesNotWriteVectorOrVersion() {
        var f = new VectorOperationFixture();
        f.coordinator.withUserLock("u", lease -> f.service.checkpoint(lease, "daily", "v1", null, null)).block();
        assertThat(f.esWrites).isZero(); assertThat(f.redis.version).isZero();
        assertThat(f.journal.get("daily").getStatus()).isEqualTo("COMPLETED");
    }
    @Test void conflictedOperationCanReprepareSameKeyAfterFreshRead() {
        var f = new VectorOperationFixture(); f.profile("first", 0).block();
        var captured = VectorOperationFixture.copyDoc(f.document);
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "daily", "LONG_TERM", captured,
                VectorOperationFixture.vector(1), "v1", null, null)).block();
        String oldId = f.journal.get("daily").getOperationId();
        f.document.setSeqNoPrimaryTerm(new SeqNoPrimaryTerm(4, 1));
        StepVerifier.create(f.coordinator.withUserLock("u", f.service::reconcilePending))
                .expectError(UserVectorStore.RetryableVectorConflictException.class).verify();
        var fresh = VectorOperationFixture.copyDoc(f.document);
        f.coordinator.withUserLock("u", lease -> f.service.applyLongTerm(lease, "daily", fresh,
                VectorOperationFixture.vector(2), "v1", null, null)).block();
        assertThat(f.journal.get("daily").getOperationId()).isNotEqualTo(oldId);
        assertThat(f.journal.get("daily").getStatus()).isEqualTo("COMPLETED");
        assertThat(f.document.getUserLongTermVector()).isEqualTo(VectorOperationFixture.vector(2));
    }
}
