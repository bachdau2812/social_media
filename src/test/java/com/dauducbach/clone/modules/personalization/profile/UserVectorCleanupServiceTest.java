package com.dauducbach.clone.modules.personalization.profile;

import com.dauducbach.clone.modules.personalization.model.UserDetailVector;
import com.dauducbach.clone.modules.personalization.support.VectorOperationFixture;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class UserVectorCleanupServiceTest {
    @Test void cleanupRetainsTombstoneCancelsPendingAndRetriesAfterSqlDeletion() throws Exception {
        var f = new VectorOperationFixture();
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "pending", "PROFILE", new UserDetailVector(), VectorOperationFixture.vector(0), "profile-v1", null, null)).block();
        when(f.store.retainDeletionTombstone("u")).thenReturn(Mono.error(new IllegalStateException("ES offline")));
        var cleanup = new UserVectorCleanupService(f.store, f.service, f.coordinator, f.redis);
        StepVerifier.create(cleanup.deleteUser("u", () -> Mono.fromRunnable(() -> f.userExists = false), Mono::empty))
                .expectErrorMessage("ES offline").verify();
        assertThat(f.userExists).isFalse();
        when(f.store.retainDeletionTombstone("u")).thenReturn(Mono.empty());
        f.redis.version = 3; f.redis.committed = "old"; f.redis.shortJson = "[]"; f.redis.shortModel = "model";
        StepVerifier.create(cleanup.deleteUser("u", () -> Mono.fromRunnable(() -> f.userExists = false), Mono::empty)).verifyComplete();
        assertThat(f.redis.version).isZero(); assertThat(f.redis.committed).isNull();
        assertThat(f.redis.shortJson).isNull(); assertThat(f.redis.shortModel).isNull();
        assertThat(f.journal.get("pending").getStatus()).isEqualTo("DELETED");
        assertThat(f.redis.owner).isNull();
    }
    @Test void deletionCancellationRejectsExistingSqlUsersWithoutCancellingWork() {
        var f = new VectorOperationFixture();
        f.coordinator.withUserLock("u", lease -> f.service.prepare(lease, "pending", "PROFILE", new UserDetailVector(), VectorOperationFixture.vector(0), "profile-v1", null, null)).block();
        StepVerifier.create(f.coordinator.withUserLock("u", f.service::cancelForDeleted))
                .expectErrorMessage("Cannot cancel vector work for an existing user").verify();
        assertThat(f.journal.get("pending").getStatus()).isEqualTo("PREPARED");
    }

    @Test @SuppressWarnings({"unchecked", "rawtypes"})
    void redisCleanupGuardsOwnershipAndNeverDeletesLeaseOrFeed() {
        var template = mock(org.springframework.data.redis.core.ReactiveStringRedisTemplate.class);
        doReturn(Flux.just(-1L)).when(template).execute(any(org.springframework.data.redis.core.script.RedisScript.class), anyList(), anyList());
        var redis = new com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorRedisState(template);
        var lease = new com.dauducbach.clone.modules.personalization.infrastructure.redis.VectorLease("u", "token");
        StepVerifier.create(redis.clearDeleted(lease)).expectError(com.dauducbach.clone.modules.personalization.infrastructure.redis.UserVectorCoordinator.LeaseLostException.class).verify();
        ArgumentCaptor<org.springframework.data.redis.core.script.RedisScript> script = ArgumentCaptor.forClass(org.springframework.data.redis.core.script.RedisScript.class);
        ArgumentCaptor<List> keys = ArgumentCaptor.forClass(List.class);
        verify(template).execute(script.capture(), keys.capture(), eq(List.of("token")));
        assertThat(keys.getValue()).containsExactly("vector:lock:u", "user_vector_version:u", "feed:dirty:u", "vector:committed_operation:u", "user_short_term_vector:u", "user_short_term_vector_model:u");
        assertThat(script.getValue().getScriptAsString()).contains("if redis.call('GET',KEYS[1]) ~= ARGV[1] then return -1 end", "redis.call('DEL',KEYS[2],KEYS[3],KEYS[4],KEYS[5],KEYS[6])");
        assertThat(script.getValue().getScriptAsString()).doesNotContain("'DEL',KEYS[1]");
    }


}
