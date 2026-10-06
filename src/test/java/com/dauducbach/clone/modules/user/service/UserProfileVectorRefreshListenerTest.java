package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.*;
import com.dauducbach.clone.modules.user.repositoty.*;
import com.dauducbach.clone.utils.GetVectorEmbedding;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class UserProfileVectorRefreshListenerTest {
    final VectorOperationFixture f = new VectorOperationFixture();
    final UserJobRepository jobs = mock(UserJobRepository.class);
    final UserHighSchoolRepository schools = mock(UserHighSchoolRepository.class);
    final UserUniversityRepository universities = mock(UserUniversityRepository.class);
    final GetVectorEmbedding embedding = mock(GetVectorEmbedding.class);
    final AtomicReference<UserDetails> current = new AtomicReference<>(UserDetails.builder().userId("u").fullName("Current Name").username("bach").build());

    UserProfileVectorRefreshListener listener() {
        when(f.users.findById("u")).thenAnswer(call -> Mono.defer(() -> Mono.justOrEmpty(current.get())));
        when(jobs.findByUserId("u")).thenReturn(Flux.empty());
        when(schools.findByUserId("u")).thenReturn(Flux.empty());
        when(universities.findByUserId("u")).thenReturn(Flux.empty());
        when(embedding.getEmbedding(anyString())).thenAnswer(call -> Mono.fromSupplier(() -> {
            assertThat(f.redis.owner).as("embedding is outside the lease").isNull();
            return VectorOperationFixture.vector(call.<String>getArgument(0).contains("Changed") ? 1 : 0);
        }));
        return new UserProfileVectorRefreshListener(f.users, jobs, schools, universities, embedding, f.coordinator, f.service);
    }
    String event(String id) { return "{\"userId\":\"u\",\"eventId\":\"" + id + "\",\"operation\":\"CREATE\",\"profile\":{\"userId\":\"u\",\"fullName\":\"Stale Name\"}}"; }
    void refresh(UserProfileVectorRefreshListener listener, String id) { listener.handleProfileVectorRefreshEvent(event(id)).join(); }

    @Test void createUsesCurrentSqlAndSeedsBothVectors() {
        var listener = listener(); refresh(listener, "create");
        verify(embedding).getEmbedding("full_name: Current Name. username: bach");
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(0));
        assertThat(f.document.getUserLongTermVector()).isEqualTo(f.document.getUserVector());
        assertThat(f.redis.version).isEqualTo(1);
        assertThat(f.journal.values()).allMatch(op -> op.getStatus().equals("COMPLETED"));
    }
    @Test void createRetryAndProfileUpdatePreserveLearnedLongTerm() {
        var listener = listener(); refresh(listener, "create");
        f.document.setUserLongTermVector(VectorOperationFixture.vector(2)); f.document.setHasLearnedHistory(true);
        refresh(listener, "create");
        current.set(UserDetails.builder().userId("u").fullName("Changed Name").build());
        refresh(listener, "update");
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(1));
        assertThat(f.document.getUserLongTermVector()).isEqualTo(VectorOperationFixture.vector(2));
        assertThat(f.document.getHasLearnedHistory()).isTrue(); assertThat(f.esWrites).isEqualTo(2);
    }
    @Test void embeddingFailureFailsKafkaFutureAndSameEventRetries() {
        var listener = listener(); var failure = new IllegalStateException("provider offline");
        when(embedding.getEmbedding(anyString())).thenReturn(Mono.error(failure));
        StepVerifier.create(Mono.fromFuture(listener.handleProfileVectorRefreshEvent(event("retry"))))
                .expectErrorMatches(error -> error == failure).verify();
        assertThat(f.document).isNull(); assertThat(f.journal).isEmpty();
        when(embedding.getEmbedding(anyString())).thenReturn(Mono.just(VectorOperationFixture.vector(0)));
        refresh(listener, "retry"); assertThat(f.redis.version).isEqualTo(1);
    }
    @Test void absentSqlUserSkipsEvenOldCreatePayload() {
        var listener = listener(); current.set(null); f.userExists = false;
        refresh(listener, "old-create"); verify(embedding, never()).getEmbedding(anyString()); assertThat(f.document).isNull();
    }
    @Test void sourceChangeDuringEmbeddingRebuildsOutsideLease() {
        var listener = listener();
        when(embedding.getEmbedding(anyString())).thenAnswer(call -> Mono.fromSupplier(() -> {
            assertThat(f.redis.owner).isNull(); String text = call.getArgument(0);
            if (text.contains("Current")) { current.set(UserDetails.builder().userId("u").fullName("Changed Name").build()); return VectorOperationFixture.vector(0); }
            return VectorOperationFixture.vector(1);
        }));
        refresh(listener, "change"); assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(1));
        assertThat(f.esWrites).isEqualTo(1); verify(embedding, times(2)).getEmbedding(anyString());
    }
    @Test void eventIdentityAndFingerprintHandleRetryWithNewSourceAndAtoBtoA() {
        var listener = listener(); refresh(listener, "original");
        current.set(UserDetails.builder().userId("u").fullName("Changed Name").build()); refresh(listener, "original");
        current.set(UserDetails.builder().userId("u").fullName("Current Name").username("bach").build()); refresh(listener, "return-to-A");
        refresh(listener, "return-to-A"); assertThat(f.esWrites).isEqualTo(3); assertThat(f.redis.version).isEqualTo(3);
        assertThat(f.document.getUserVector()).isEqualTo(VectorOperationFixture.vector(0));
    }
    @Test void employmentAndEducationOrderingIsDeterministic() {
        var listener = listener();
        var a = UserJob.builder().position("A").build(); var b = UserJob.builder().position("B").build();
        var c = UserHighSchool.builder().schoolName("C").build(); var d = UserHighSchool.builder().schoolName("D").build();
        var e = UserUniversity.builder().schoolName("E").build(); var g = UserUniversity.builder().schoolName("G").build();
        when(jobs.findByUserId("u")).thenReturn(Flux.just(b,a)); when(schools.findByUserId("u")).thenReturn(Flux.just(d,c)); when(universities.findByUserId("u")).thenReturn(Flux.just(g,e));
        String first = listener.buildProfileText("u").block();
        when(jobs.findByUserId("u")).thenReturn(Flux.just(a,b)); when(schools.findByUserId("u")).thenReturn(Flux.just(c,d)); when(universities.findByUserId("u")).thenReturn(Flux.just(e,g));
        assertThat(listener.buildProfileText("u").block()).isEqualTo(first);
    }
    @Test void ambiguousEsFailureRetriesDurableOperationWithoutExtraVersion() {
        var listener = listener(); f.failApplied = true;
        StepVerifier.create(Mono.fromFuture(listener.handleProfileVectorRefreshEvent(event("crash")))).expectError().verify();
        assertThat(f.journal.values()).anyMatch(op -> op.getStatus().equals("PREPARED"));
        f.failApplied = false; refresh(listener, "crash"); assertThat(f.esWrites).isEqualTo(1); assertThat(f.redis.version).isEqualTo(1);
    }
    @Test void sameEventIdAndProfileTextForDifferentUsersHaveDifferentOperationKeys() {
        var service = mock(UserVectorOperationService.class);
        when(f.users.findById(anyString())).thenReturn(Mono.just(current.get()));
        when(jobs.findByUserId(anyString())).thenReturn(Flux.empty());
        when(schools.findByUserId(anyString())).thenReturn(Flux.empty());
        when(universities.findByUserId(anyString())).thenReturn(Flux.empty());
        when(embedding.getEmbedding(anyString())).thenReturn(Mono.just(VectorOperationFixture.vector(0)));
        when(service.applyProfile(any(), anyString(), anyList())).thenReturn(Mono.just(new UserVectorUpdateOperation()));
        var listener = new UserProfileVectorRefreshListener(f.users, jobs, schools, universities, embedding, f.coordinator, service);
        refresh(listener, "shared-event");
        listener.handleProfileVectorRefreshEvent(event("shared-event").replace("\"u\"", "\"v\"")).join();
        var keys = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(service, times(2)).applyProfile(any(), keys.capture(), anyList());
        assertThat(keys.getAllValues()).doesNotHaveDuplicates();
    }

}
