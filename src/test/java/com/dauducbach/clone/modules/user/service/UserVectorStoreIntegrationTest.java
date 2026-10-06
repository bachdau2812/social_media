package com.dauducbach.clone.modules.user.service;

import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchClients;
import reactor.test.StepVerifier;
import java.util.Map;
import java.util.UUID;
import static org.assertj.core.api.Assertions.*;

/** Requires an explicitly disposable local ES instance; never loads application credentials/endpoints. */
@EnabledIfEnvironmentVariable(named = "VECTOR_TEST_ES_DISPOSABLE", matches = "true")
@EnabledIfEnvironmentVariable(named = "VECTOR_TEST_ES_PORT", matches = "[0-9]+")
class UserVectorStoreIntegrationTest {
    @Test @SuppressWarnings("rawtypes")
    void realScriptsPreserveLearnedVectorAndRejectStaleLeaseWrites() throws Exception {
        var client = ElasticsearchClients.createReactive(ClientConfiguration.builder()
                .connectedTo("127.0.0.1:" + System.getenv("VECTOR_TEST_ES_PORT")).build());
        var store = new UserVectorStore(client); String user = "vector-it-" + UUID.randomUUID();
        var get = GetRequest.of(builder -> builder.index("user_detail_vector").id(user));
        try {
            store.upsertProfileAndSeedLongTerm(user, VectorOperationFixture.vector(0), "create", null, null).block();
            var seeded = client.get(get, Map.class).block();
            StepVerifier.create(store.upsertProfileAndSeedLongTerm(user, VectorOperationFixture.vector(2), "late-create", null, null))
                    .expectError(UserVectorStore.RetryableVectorConflictException.class).verify();
            store.updateLongTerm(user, VectorOperationFixture.vector(1), "learn", seeded.seqNo(), seeded.primaryTerm()).block();
            // Simulate a lost response: retrying original tokens conflicts, operation ID proves prior application.
            StepVerifier.create(store.updateLongTerm(user, VectorOperationFixture.vector(1), "learn", seeded.seqNo(), seeded.primaryTerm()))
                    .expectError(UserVectorStore.RetryableVectorConflictException.class).verify();
            var learned = client.get(get, Map.class).block();
            assertThat(learned.source()).containsEntry("user_long_term_operation_id", "learn");
            StepVerifier.create(store.upsertProfileAndSeedLongTerm(user, VectorOperationFixture.vector(2), "stale-profile", seeded.seqNo(), seeded.primaryTerm()))
                    .expectError(UserVectorStore.RetryableVectorConflictException.class).verify();
            store.upsertProfileAndSeedLongTerm(user, VectorOperationFixture.vector(2), "profile", learned.seqNo(), learned.primaryTerm()).block();
            var result = client.get(get, Map.class).block().source();
            assertThat(result).containsEntry("user_long_term_operation_id", "learn").containsEntry("has_learned_history", true)
                    .containsEntry("user_profile_operation_id", "profile");
            assertThat((java.util.List) result.get("user_long_term_vector")).isEqualTo(VectorOperationFixture.vector(1));
        } finally {
            client.delete(DeleteRequest.of(builder -> builder.index("user_detail_vector").id(user))).block();
            client._transport().close();
        }
    }
}
