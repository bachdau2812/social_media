package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.audit.service.UserAuditService;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;
import reactor.test.StepVerifier;
import java.util.List;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class UserVectorLifecycleTest {
    @Test @SuppressWarnings({"rawtypes", "unchecked"})
    void publisherIncludesFullNameAndStableCreateIdentity() {
        KafkaSender<String,String> sender = mock(KafkaSender.class);
        when(sender.send(any())).thenReturn(Flux.empty());
        var publisher = new UserProfileVectorEventPublisher(sender);
        var details = UserDetails.builder().userId("u").fullName("Current Name").build();
        publisher.publishRefreshEventForCreatedUser("u", "USER_DETAILS", "CREATE", "u", details).block();
        publisher.publishRefreshEventForCreatedUser("u", "USER_DETAILS", "CREATE", "u", details).block();
        ArgumentCaptor<org.reactivestreams.Publisher> records = ArgumentCaptor.forClass(org.reactivestreams.Publisher.class);
        verify(sender, times(2)).send(records.capture());
        SenderRecord first = (SenderRecord) Flux.from(records.getAllValues().get(0)).blockFirst();
        SenderRecord second = (SenderRecord) Flux.from(records.getAllValues().get(1)).blockFirst();
        JsonObject a = com.dauducbach.clone.utils.GsonUtils.fromString((String) first.value());
        JsonObject b = com.dauducbach.clone.utils.GsonUtils.fromString((String) second.value());
        assertThat(a.getAsJsonObject("profile").get("fullName")).isNotNull();
        assertThat(a.getAsJsonObject("profile").get("fullName").getAsString()).isEqualTo("Current Name");
        assertThat(a.get("eventId")).isEqualTo(b.get("eventId"));
    }

    @Test @SuppressWarnings({"rawtypes", "unchecked"})
    void publisherPropagatesRejectedBrokerAcknowledgement() {
        KafkaSender<String,String> sender = mock(KafkaSender.class);
        SenderResult<String> result = mock(SenderResult.class);
        var error = new IllegalStateException("broker rejected");
        when(result.exception()).thenReturn(error);
        when(sender.<String>send(any())).thenReturn(Flux.just(result));
        StepVerifier.create(new UserProfileVectorEventPublisher(sender).publishRefreshEvent("u", "USER_DETAILS", "UPDATE", "u"))
                .expectErrorMatches(actual -> actual == error).verify();
    }

    @Test @SuppressWarnings("unchecked")
    void creationReplayPublishesCurrentSqlProfileWithoutInsertingAgain() {
        var users = mock(UserDetailsRepository.class);
        var template = mock(R2dbcEntityTemplate.class, RETURNS_DEEP_STUBS);
        var redis = mock(ReactiveRedisTemplate.class, RETURNS_DEEP_STUBS);
        var publisher = mock(UserProfileVectorEventPublisher.class);
        var current = UserDetails.builder().userId("u").fullName("Edited Name").build();
        when(users.findById("u")).thenReturn(Mono.just(current));
        when(template.insert(UserDetails.class).using(any(UserDetails.class))).thenReturn(Mono.error(new IllegalStateException("duplicate insert")));
        when(publisher.publishRefreshEventForCreatedUser("u", "USER_DETAILS", "CREATE", "u", current)).thenReturn(Mono.empty());
        var service = new UserDetailsService(users, template, redis, mock(UserAuditService.class), publisher, mock(UserVectorCleanupService.class));
        StepVerifier.create(Mono.fromFuture(service.createUserDetails("{\"userId\":\"u\",\"fullName\":\"Stale Name\"}"))).verifyComplete();
        verify(publisher).publishRefreshEventForCreatedUser("u", "USER_DETAILS", "CREATE", "u", current);
    }
    @Test @SuppressWarnings("unchecked")
    void insertSuccessPublishFailureThenCreationReplayRetriesCurrentProfile() {
        var users = mock(UserDetailsRepository.class);
        var template = mock(R2dbcEntityTemplate.class, RETURNS_DEEP_STUBS);
        var redis = mock(ReactiveRedisTemplate.class, RETURNS_DEEP_STUBS);
        var publisher = mock(UserProfileVectorEventPublisher.class);
        var row = new java.util.concurrent.atomic.AtomicReference<UserDetails>();
        when(users.findById("u")).thenAnswer(call -> Mono.defer(() -> Mono.justOrEmpty(row.get())));
        when(template.insert(UserDetails.class).using(any(UserDetails.class))).thenAnswer(call -> Mono.fromSupplier(() -> { row.set(call.getArgument(0)); return row.get(); }));
        when(redis.opsForValue().set(anyString(), anyString(), any(java.time.Duration.class))).thenReturn(Mono.just(true));
        when(publisher.publishRefreshEventForCreatedUser(eq("u"), anyString(), eq("CREATE"), eq("u"), any()))
                .thenReturn(Mono.error(new IllegalStateException("publish failed")), Mono.empty());
        var service = new UserDetailsService(users, template, redis, mock(UserAuditService.class), publisher, mock(UserVectorCleanupService.class));
        String payload = "{\"userId\":\"u\",\"fullName\":\"Original\"}";
        StepVerifier.create(Mono.fromFuture(service.createUserDetails(payload))).expectErrorMessage("publish failed").verify();
        row.get().setFullName("Edited");
        StepVerifier.create(Mono.fromFuture(service.createUserDetails(payload))).verifyComplete();
        assertThat(row.get().getFullName()).isEqualTo("Edited");
        verify(template.insert(UserDetails.class), times(1)).using(any(UserDetails.class));
        verify(publisher, times(2)).publishRefreshEventForCreatedUser(eq("u"), anyString(), eq("CREATE"), eq("u"), any());
    }

    @Test @SuppressWarnings("unchecked")
    void deletedSqlUserCleanupMustBeRetryable() {
        var users = mock(UserDetailsRepository.class);
        when(users.findById("u")).thenReturn(Mono.empty());
        when(users.deleteById("u")).thenReturn(Mono.empty());
        var redis = mock(ReactiveRedisTemplate.class, RETURNS_DEEP_STUBS);
        when(redis.opsForValue().delete(anyString())).thenReturn(Mono.just(true));
        var cleanup = mock(UserVectorCleanupService.class);
        when(cleanup.deleteUser(eq("u"), any())).thenAnswer(call -> {
            java.util.function.Supplier<Mono<Void>> afterSql = call.getArgument(1);
            return Mono.defer(afterSql);
        });
        var service = new UserDetailsService(users, mock(R2dbcEntityTemplate.class), redis,
                mock(UserAuditService.class), mock(UserProfileVectorEventPublisher.class), cleanup);
        StepVerifier.create(service.deleteUserDetails("u")).verifyComplete();
    }

    @Test @SuppressWarnings("unchecked")
    void employmentAndEducationDeletesPublishRefreshAndPropagatePublishFailure() {
        var template = mock(R2dbcEntityTemplate.class);
        var redis = mock(ReactiveRedisTemplate.class, RETURNS_DEEP_STUBS);
        when(redis.opsForValue().delete(anyString())).thenReturn(Mono.just(true));
        var audit = mock(UserAuditService.class);
        var publisher = mock(UserProfileVectorEventPublisher.class);
        when(publisher.publishRefreshEvent(eq("u"), anyString(), eq("DELETE"), eq("component")))
                .thenReturn(Mono.error(new IllegalStateException("publish failed")));
        var jobs = mock(com.dauducbach.clone.modules.user.repositoty.UserJobRepository.class);
        var schools = mock(com.dauducbach.clone.modules.user.repositoty.UserHighSchoolRepository.class);
        var universities = mock(com.dauducbach.clone.modules.user.repositoty.UserUniversityRepository.class);
        when(jobs.findById("component")).thenReturn(Mono.just(com.dauducbach.clone.modules.user.entity.UserJob.builder().id("component").userId("u").build()));
        when(jobs.deleteById("component")).thenReturn(Mono.empty());
        when(schools.findById("component")).thenReturn(Mono.just(com.dauducbach.clone.modules.user.entity.UserHighSchool.builder().id("component").userId("u").build()));
        when(schools.deleteById("component")).thenReturn(Mono.empty());
        when(universities.findById("component")).thenReturn(Mono.just(com.dauducbach.clone.modules.user.entity.UserUniversity.builder().id("component").userId("u").build()));
        when(universities.deleteById("component")).thenReturn(Mono.empty());
        StepVerifier.create(new UserJobService(jobs, template, redis, audit, publisher).deleteUserJob("component"))
                .expectErrorMatches(error -> error.getCause() != null && error.getCause().getMessage().equals("publish failed")).verify();
        StepVerifier.create(new UserHighSchoolService(schools, template, redis, audit, publisher).deleteUserHighSchool("component"))
                .expectErrorMatches(error -> error.getCause() != null && error.getCause().getMessage().equals("publish failed")).verify();
        StepVerifier.create(new UserUniversityService(universities, template, redis, audit, publisher).deleteUserUniversity("component"))
                .expectErrorMatches(error -> error.getCause() != null && error.getCause().getMessage().equals("publish failed")).verify();
        for (String prefix : List.of("user_job", "user_high_school", "user_university")) {
            verify(redis.opsForValue()).delete(prefix + ":component");
            verify(redis.opsForValue()).delete(prefix + "_list:u");
        }
        verify(publisher).publishRefreshEvent("u", "USER_JOB", "DELETE", "component");
        verify(publisher).publishRefreshEvent("u", "USER_HIGH_SCHOOL", "DELETE", "component");
        verify(publisher).publishRefreshEvent("u", "USER_UNIVERSITY", "DELETE", "component");
    }

    @Test void cleanupRetainsTombstoneCancelsPendingAndRetriesAfterSqlDeletion() throws Exception {
        var f = new VectorOperationFixture();
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "pending", "PROFILE", new com.dauducbach.clone.modules.user.entity.UserDetailVector(), VectorOperationFixture.vector(0), "profile-v1", null, null)).block();
        when(f.users.deleteById("u")).thenAnswer(call -> Mono.fromRunnable(() -> f.userExists = false));
        when(f.store.retainDeletionTombstone("u")).thenReturn(Mono.error(new IllegalStateException("ES offline")));
        var cleanup = new UserVectorCleanupService(f.users, f.store, f.service, f.coordinator, f.redis);
        StepVerifier.create(cleanup.deleteUser("u")).expectErrorMessage("ES offline").verify();
        assertThat(f.userExists).isFalse();
        when(f.store.retainDeletionTombstone("u")).thenReturn(Mono.empty());
        f.redis.version = 3; f.redis.committed = "old"; f.redis.shortJson = "[]"; f.redis.shortModel = "model";
        StepVerifier.create(cleanup.deleteUser("u")).verifyComplete();
        assertThat(f.redis.version).isZero(); assertThat(f.redis.committed).isNull();
        assertThat(f.redis.shortJson).isNull(); assertThat(f.redis.shortModel).isNull();
        assertThat(f.journal.get("pending").getStatus()).isEqualTo("DELETED");
        assertThat(f.redis.owner).isNull();
    }
    @Test void deletionCancellationRejectsExistingSqlUsersWithoutCancellingWork() {
        var f = new VectorOperationFixture();
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "pending", "PROFILE", new com.dauducbach.clone.modules.user.entity.UserDetailVector(), VectorOperationFixture.vector(0), "profile-v1", null, null)).block();
        StepVerifier.create(f.coordinator.withUserLock("u", f.service::cancelForDeleted))
                .expectErrorMessage("Cannot cancel vector work for an existing user").verify();
        assertThat(f.journal.get("pending").getStatus()).isEqualTo("PREPARED");
    }

    @Test @SuppressWarnings({"unchecked", "rawtypes"})
    void redisCleanupGuardsOwnershipAndNeverDeletesLeaseOrFeed() {
        var template = mock(org.springframework.data.redis.core.ReactiveStringRedisTemplate.class);
        doReturn(Flux.just(-1L)).when(template).execute(any(org.springframework.data.redis.core.script.RedisScript.class), anyList(), anyList());
        var redis = new com.dauducbach.clone.infrastructure.vector.VectorRedisState(template);
        var lease = new com.dauducbach.clone.infrastructure.vector.VectorLease("u", "token");
        StepVerifier.create(redis.clearDeleted(lease)).expectError(com.dauducbach.clone.infrastructure.vector.UserVectorCoordinator.LeaseLostException.class).verify();
        ArgumentCaptor<org.springframework.data.redis.core.script.RedisScript> script = ArgumentCaptor.forClass(org.springframework.data.redis.core.script.RedisScript.class);
        ArgumentCaptor<List> keys = ArgumentCaptor.forClass(List.class);
        verify(template).execute(script.capture(), keys.capture(), eq(List.of("token")));
        assertThat(keys.getValue()).containsExactly("vector:lock:u", "user_vector_version:u", "feed:dirty:u", "vector:committed_operation:u", "user_short_term_vector:u", "user_short_term_vector_model:u");
        assertThat(script.getValue().getScriptAsString()).contains("if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end", "redis.call('DEL',KEYS[2],KEYS[3],KEYS[4],KEYS[5],KEYS[6])");
        assertThat(script.getValue().getScriptAsString()).doesNotContain("'DEL',KEYS[1]");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(ProfileCacheFixture.Component.class)
    void componentCreateSqlSuccessMaintainsSeededCacheDespitePublishFailure(ProfileCacheFixture.Component component) {
        assertComponentCacheOnPublishFailure(component, "CREATE");
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(ProfileCacheFixture.Component.class)
    void componentUpdateSqlSuccessMaintainsSeededCacheDespitePublishFailure(ProfileCacheFixture.Component component) {
        assertComponentCacheOnPublishFailure(component, "UPDATE");
    }
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.EnumSource(ProfileCacheFixture.Component.class)
    void componentDeleteSqlSuccessEvictsSeededCacheDespitePublishFailure(ProfileCacheFixture.Component component) {
        assertComponentCacheOnPublishFailure(component, "DELETE");
    }
    private void assertComponentCacheOnPublishFailure(ProfileCacheFixture.Component component, String operation) {
        var f = new ProfileCacheFixture();
        Mono<?> mutation = f.componentMutation(component, operation);
        assertThat(f.cache).containsKeys(component.prefix + ":component", component.prefix + "_list:u");
        StepVerifier.create(mutation).expectErrorMatches(error -> error == f.publishFailure || error.getCause() == f.publishFailure).verify();
        assertThat(f.sqlMutations).isEqualTo(1);
        assertThat(f.cache).doesNotContainKey(component.prefix + "_list:u");
        if (operation.equals("DELETE")) {
            assertThat(f.sqlRow).isNull(); assertThat(f.cache).doesNotContainKey(component.prefix + ":component");
        } else {
            String key = component.prefix + ":" + f.componentId();
            assertThat(f.cache.get(key)).isEqualTo(com.dauducbach.clone.utils.RedisUtil.serialize(f.sqlRow)).contains("After");
            assertThat(f.ttls.get(key)).isEqualTo(java.time.Duration.ofHours(24));
        }
    }
    @Test void profileInsertSqlSuccessReplacesStaleCachedDetailsDespitePublishFailure() {
        var f = new ProfileCacheFixture();
        var row = UserDetails.builder().userId("u").fullName("After").build();
        f.seed("user_details_info:u", UserDetails.builder().userId("u").fullName("Before").build());
        f.insertion(UserDetails.class);
        var service = new UserDetailsService(mock(UserDetailsRepository.class), f.template, f.redis, f.audit, f.publisher, mock(UserVectorCleanupService.class));
        StepVerifier.create(service.insertUserDetails(row)).expectErrorMatches(error -> error == f.publishFailure).verify();
        assertThat(f.sqlMutations).isEqualTo(1);
        assertThat(f.cache.get("user_details_info:u")).isEqualTo(com.dauducbach.clone.utils.RedisUtil.serialize(row)).contains("After");
        assertThat(f.ttls.get("user_details_info:u")).isEqualTo(java.time.Duration.ofHours(24));
    }
    @Test void profileUpdateSqlSuccessReplacesStaleCachedDetailsDespitePublishFailure() {
        var f = new ProfileCacheFixture();
        var users = mock(UserDetailsRepository.class);
        var row = UserDetails.builder().userId("u").fullName("Before").build();
        f.sqlRow = row; f.seed("user_details_info:u", row);
        when(users.existsById("u")).thenReturn(Mono.just(true));
        when(users.findById("u")).thenReturn(Mono.just(row));
        when(users.save(any())).thenAnswer(call -> Mono.fromSupplier(() -> { f.sqlRow = call.getArgument(0); f.sqlMutations++; return (UserDetails) f.sqlRow; }));
        var service = new UserDetailsService(users, f.template, f.redis, f.audit, f.publisher, mock(UserVectorCleanupService.class));
        StepVerifier.create(service.updateUserDetails(com.dauducbach.clone.modules.user.dto.request.UserDetailsUpdateRequest.builder().userId("u").fullName("After").build()))
                .expectErrorMatches(error -> error.getCause() == f.publishFailure).verify();
        assertThat(f.sqlMutations).isEqualTo(1);
        assertThat(f.cache.get("user_details_info:u")).isEqualTo(com.dauducbach.clone.utils.RedisUtil.serialize(f.sqlRow)).contains("After");
        assertThat(f.ttls.get("user_details_info:u")).isEqualTo(java.time.Duration.ofHours(24));
    }
    @Test void profileDeletionSqlSuccessEvictsStaleCachedDetailsEvenWhenEsCleanupFails() {
        var cache = new ProfileCacheFixture(); var f = new VectorOperationFixture();
        var row = UserDetails.builder().userId("u").fullName("Before").build();
        cache.seed("user_details_info:u", row);
        when(f.users.findById("u")).thenAnswer(call -> Mono.defer(() -> f.userExists ? Mono.just(row) : Mono.empty()));
        when(f.users.deleteById("u")).thenReturn(Mono.fromRunnable(() -> f.userExists = false));
        var failure = new IllegalStateException("ES cleanup failed");
        when(f.store.retainDeletionTombstone("u")).thenReturn(Mono.error(failure));
        var cleanup = new UserVectorCleanupService(f.users, f.store, f.service, f.coordinator, f.redis);
        var service = new UserDetailsService(f.users, cache.template, cache.redis, cache.audit, cache.publisher, cleanup);
        StepVerifier.create(service.getUserDetailsById("u")).assertNext(cached -> assertThat(cached.getFullName()).isEqualTo("Before")).verifyComplete();
        StepVerifier.create(service.deleteUserDetails("u")).expectErrorMatches(error -> error.getCause() == failure).verify();
        assertThat(f.userExists).isFalse(); assertThat(cache.cache).doesNotContainKey("user_details_info:u");
        StepVerifier.create(service.getUserDetailsById("u")).expectError(com.dauducbach.clone.commons.exception.AppException.class).verify();
        assertThat(f.redis.owner).isNull();
    }

}
