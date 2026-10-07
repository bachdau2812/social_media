package com.dauducbach.clone.modules.personalization.incoming;

import com.dauducbach.clone.modules.personalization.incoming.UserProfileVectorRefreshListener;
import com.dauducbach.clone.modules.personalization.model.UserVectorUpdateOperation;
import com.dauducbach.clone.modules.personalization.profile.UserProfileVectorRefreshUseCase;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.personalization.journal.UserVectorOperationService;
import com.dauducbach.clone.modules.personalization.support.VectorOperationFixture;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.HighSchoolSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.JobSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.ProfileContentSnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UniversitySnapshot;
import com.dauducbach.clone.modules.user.publicapi.UserProfileQuery.UserDetailsSnapshot;
import com.dauducbach.clone.modules.embedding.publicapi.TextEmbeddingProvider;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class UserProfileVectorRefreshListenerTest {
    final VectorOperationFixture f = new VectorOperationFixture();
    final UserProfileQuery profiles = mock(UserProfileQuery.class);
    final TextEmbeddingProvider embedding = mock(TextEmbeddingProvider.class);
    final AtomicReference<ProfileContentSnapshot> current = new AtomicReference<>(profile("Current Name", "bach"));

    UserProfileVectorRefreshUseCase useCase() {
        when(profiles.getProfileContentSnapshot(anyString()))
                .thenAnswer(call -> Mono.defer(() -> Mono.justOrEmpty(current.get())));
        when(embedding.getEmbedding(anyString())).thenAnswer(call -> Mono.fromSupplier(() -> {
            assertThat(f.redis.owner).as("embedding is outside the lease").isNull();
            return VectorOperationFixture.vector(call.<String>getArgument(0).contains("Changed") ? 1 : 0);
        }));
        return new UserProfileVectorRefreshUseCase(profiles, embedding, f.coordinator, f.recovery, f.service);
    }

    UserProfileVectorRefreshListener listener() {
        return new UserProfileVectorRefreshListener(useCase());
    }

    String event(String id) {
        return "{\"userId\":\"u\",\"eventId\":\"" + id
                + "\",\"operation\":\"CREATE\",\"profile\":{\"userId\":\"u\",\"fullName\":\"Stale Name\"}}";
    }

    void refresh(UserProfileVectorRefreshListener listener, String id) {
        listener.handleProfileVectorRefreshEvent(event(id)).join();
    }

    @Test
    void createUsesCurrentProfileSnapshotAndSeedsBothVectors() {
        var listener = listener();
        refresh(listener, "create");
        verify(embedding).getEmbedding("full_name: Current Name. username: bach");
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(0));
        assertThat(f.document.getUserLongTermVector()).isEqualTo(f.document.getUserVector());
        assertThat(f.redis.version).isEqualTo(1);
        assertThat(f.journal.values()).allMatch(op -> op.getStatus().equals("COMPLETED"));
    }

    @Test
    void createRetryAndProfileUpdatePreserveLearnedLongTerm() {
        var listener = listener();
        refresh(listener, "create");
        f.document.setUserLongTermVector(VectorOperationFixture.vector(2));
        f.document.setHasLearnedHistory(true);
        refresh(listener, "create");
        current.set(profile("Changed Name", null));
        refresh(listener, "update");
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(1));
        assertThat(f.document.getUserLongTermVector()).isEqualTo(VectorOperationFixture.vector(2));
        assertThat(f.document.getHasLearnedHistory()).isTrue();
        assertThat(f.esWrites).isEqualTo(2);
    }

    @Test
    void embeddingFailureFailsKafkaFutureAndSameEventRetries() {
        var listener = listener();
        var failure = new IllegalStateException("provider offline");
        when(embedding.getEmbedding(anyString())).thenReturn(Mono.error(failure));
        StepVerifier.create(Mono.fromFuture(listener.handleProfileVectorRefreshEvent(event("retry"))))
                .expectErrorMatches(error -> error == failure).verify();
        assertThat(f.document).isNull();
        assertThat(f.journal).isEmpty();
        when(embedding.getEmbedding(anyString())).thenReturn(Mono.just(VectorOperationFixture.vector(0)));
        refresh(listener, "retry");
        assertThat(f.redis.version).isEqualTo(1);
    }

    @Test
    void absentUserSkipsEvenOldCreatePayload() {
        var listener = listener();
        current.set(null);
        f.userExists = false;
        refresh(listener, "old-create");
        verify(embedding, never()).getEmbedding(anyString());
        assertThat(f.document).isNull();
    }

    @Test
    void sourceChangeDuringEmbeddingRebuildsOutsideLease() {
        var listener = listener();
        when(embedding.getEmbedding(anyString())).thenAnswer(call -> Mono.fromSupplier(() -> {
            assertThat(f.redis.owner).isNull();
            String text = call.getArgument(0);
            if (text.contains("Current")) {
                current.set(profile("Changed Name", null));
                return VectorOperationFixture.vector(0);
            }
            return VectorOperationFixture.vector(1);
        }));
        refresh(listener, "change");
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(1));
        assertThat(f.esWrites).isEqualTo(1);
        verify(embedding, times(2)).getEmbedding(anyString());
    }

    @Test
    void eventIdentityAndFingerprintHandleRetryWithNewSourceAndAtoBtoA() {
        var listener = listener();
        refresh(listener, "original");
        current.set(profile("Changed Name", null));
        refresh(listener, "original");
        current.set(profile("Current Name", "bach"));
        refresh(listener, "return-to-A");
        refresh(listener, "return-to-A");
        assertThat(f.esWrites).isEqualTo(3);
        assertThat(f.redis.version).isEqualTo(3);
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(0));
    }

    @Test
    void employmentAndEducationOrderingIsDeterministic() {
        var initial = profile("Current Name", "bach");
        current.set(new ProfileContentSnapshot(initial.user(),
                List.of(job("B"), job("A")),
                List.of(university("G"), university("E")),
                List.of(highSchool("D"), highSchool("C"))));
        var listener = listener();
        refresh(listener, "ordered");
        var text = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(embedding).getEmbedding(text.capture());
        assertThat(text.getValue()).contains("job: A. job: B")
                .contains("high_school: C, not graduated. high_school: D, not graduated")
                .contains("university: E, not graduated. university: G, not graduated");
    }

    @Test
    void ambiguousEsFailureRetriesDurableOperationWithoutExtraVersion() {
        var listener = listener();
        f.failApplied = true;
        StepVerifier.create(Mono.fromFuture(listener.handleProfileVectorRefreshEvent(event("crash"))))
                .expectError().verify();
        assertThat(f.journal.values()).anyMatch(op -> op.getStatus().equals("PREPARED"));
        f.failApplied = false;
        refresh(listener, "crash");
        assertThat(f.esWrites).isEqualTo(1);
        assertThat(f.redis.version).isEqualTo(1);
    }

    @Test
    void sameEventIdAndProfileTextForDifferentUsersHaveDifferentOperationKeys() {
        var service = mock(UserVectorOperationService.class);
        when(profiles.getProfileContentSnapshot(anyString())).thenReturn(Mono.just(profile("Current Name", "bach")));
        when(embedding.getEmbedding(anyString())).thenReturn(Mono.just(VectorOperationFixture.vector(0)));
        when(service.reconcilePending(any())).thenReturn(Mono.empty());
        when(service.applyProfile(any(), anyString(), anyList())).thenReturn(Mono.just(new UserVectorUpdateOperation()));
        var recovery = new PreferenceRecoveryCoordinator(service, f.processing);
        var useCase = new UserProfileVectorRefreshUseCase(profiles, embedding, f.coordinator, recovery, service);
        var listener = new UserProfileVectorRefreshListener(useCase);
        refresh(listener, "shared-event");
        listener.handleProfileVectorRefreshEvent(event("shared-event").replace("\"u\"", "\"v\"")).join();
        var keys = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(service, times(2)).applyProfile(any(), keys.capture(), anyList());
        assertThat(keys.getAllValues()).doesNotHaveDuplicates();
    }

    private static ProfileContentSnapshot profile(String fullName, String username) {
        return new ProfileContentSnapshot(
                new UserDetailsSnapshot("u", username, fullName, null, null, null, null, List.of()),
                List.of(), List.of(), List.of());
    }

    private static JobSnapshot job(String position) {
        return new JobSnapshot(position, "u", null, position, null, null, true);
    }

    private static HighSchoolSnapshot highSchool(String name) {
        return new HighSchoolSnapshot(name, "u", name, null, null, false, true);
    }

    private static UniversitySnapshot university(String name) {
        return new UniversitySnapshot(name, "u", name, null, null, null, false, true);
    }
}
