package com.dauducbach.clone.modules.personalization.recovery;

import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorRedisState;
import com.dauducbach.clone.modules.personalization.infrastructure.elasticsearch.UserVectorStore;
import com.dauducbach.clone.modules.personalization.infrastructure.persistence.UserVectorUpdateOperationRepository;
import com.dauducbach.clone.modules.personalization.interactions.PreferenceInteractionRecovery;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.recovery.UserVectorContinuityService;
import com.dauducbach.clone.modules.personalization.snapshots.UserVectorQueryService;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PreferenceRecoveryCoordinatorTest {
    @Test
    void reconcilesJournalThenShortTermAndChecksShortTermContinuityOnBothSidesOfReplay() {
        ArrayList<String> events = new ArrayList<>();
        UserVectorUpdateOperationRepository journalRows = mock(UserVectorUpdateOperationRepository.class);
        UserExistenceQuery users = mock(UserExistenceQuery.class);
        UserVectorQueryService query = mock(UserVectorQueryService.class);
        UserVectorStore store = mock(UserVectorStore.class);
        UserVectorCoordinator vectorCoordinator = mock(UserVectorCoordinator.class);
        VectorRedisState redis = mock(VectorRedisState.class);
        UserVectorContinuityService vectorContinuity = mock(UserVectorContinuityService.class);
        PreferenceInteractionRecovery shortTerm = mock(PreferenceInteractionRecovery.class);
        VectorLease lease = new VectorLease("user-1", "lease-token");
        when(vectorCoordinator.requireOwner(lease)).thenReturn(Mono.empty());
        when(journalRows.findPending(lease.userId())).thenAnswer(ignored -> Flux.defer(() -> {
            events.add("journal.replay");
            return Flux.empty();
        }));
        when(vectorContinuity.requireContinuity(lease)).thenAnswer(ignored -> Mono.fromRunnable(
                () -> events.add("journal.continuity")));
        AtomicInteger continuityCheck = new AtomicInteger();
        when(shortTerm.requireContinuity(lease)).thenAnswer(ignored -> Mono.fromRunnable(
                () -> events.add("short-term.continuity." + continuityCheck.incrementAndGet())));
        when(shortTerm.reconcilePending(lease)).thenAnswer(ignored -> Mono.fromRunnable(
                () -> events.add("short-term.replay")));
        UserVectorOperationService journal = new UserVectorOperationService(
                journalRows, users, query, store, vectorCoordinator, redis, vectorContinuity);
        PreferenceRecoveryCoordinator coordinator = new PreferenceRecoveryCoordinator(journal, shortTerm);

        StepVerifier.create(coordinator.reconcilePending(lease)).verifyComplete();

        assertThat(events).containsExactly(
                "journal.replay",
                "journal.continuity",
                "short-term.continuity.1",
                "short-term.replay",
                "short-term.continuity.2");
    }

    @Test
    void stopsRecoveryWhenJournalReplayFails() {
        UserVectorOperationService journal = mock(UserVectorOperationService.class);
        PreferenceInteractionRecovery shortTerm = mock(PreferenceInteractionRecovery.class);
        VectorLease lease = new VectorLease("user-1", "lease-token");
        IllegalStateException failure = new IllegalStateException("journal unavailable");
        when(journal.reconcilePending(lease)).thenReturn(Mono.error(failure));
        PreferenceRecoveryCoordinator coordinator = new PreferenceRecoveryCoordinator(journal, shortTerm);

        StepVerifier.create(coordinator.reconcilePending(lease)).expectErrorMatches(error -> error == failure).verify();

        org.mockito.Mockito.verifyNoInteractions(shortTerm);
    }

    @Test
    void doesNotReadContinuityUntilShortTermReplayCompletes() {
        UserVectorOperationService journal = mock(UserVectorOperationService.class);
        PreferenceInteractionRecovery shortTerm = mock(PreferenceInteractionRecovery.class);
        VectorLease lease = new VectorLease("user-1", "lease-token");
        IllegalStateException failure = new IllegalStateException("short-term replay unavailable");
        when(journal.reconcilePending(lease)).thenReturn(Mono.empty());
        when(shortTerm.requireContinuity(lease)).thenReturn(Mono.empty());
        when(shortTerm.reconcilePending(lease)).thenReturn(Mono.error(failure));
        PreferenceRecoveryCoordinator coordinator = new PreferenceRecoveryCoordinator(journal, shortTerm);

        StepVerifier.create(coordinator.reconcilePending(lease)).expectErrorMatches(error -> error == failure).verify();

        verify(shortTerm).requireContinuity(lease);
        verify(shortTerm).reconcilePending(lease);
        org.mockito.Mockito.verifyNoMoreInteractions(shortTerm);
    }
}
