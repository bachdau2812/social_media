package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.infrastructure.outbox.InteractionOutbox;
import com.dauducbach.clone.infrastructure.outbox.OutboxRepository;
import com.dauducbach.clone.modules.post.dto.request.PostInteractionRequest;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repository.PostInteractionReceiptRepository;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Own disposable database; never creates/changes tables in the application's database. */
@EnabledIfEnvironmentVariable(named = "POPULARITY_TEST_MYSQL_PORT", matches = "[0-9]+")
class PostInteractionDatabaseIntegrationTest {
    @Test
    void concurrentRetriesCommitOneEventAndOutboxFailureRollsBackReceipt() {
        String database = "test_popularity_" + UUID.randomUUID().toString().replace("-", "");
        DatabaseClient admin = DatabaseClient.create(connectionFactory(null));
        admin.sql("CREATE DATABASE `" + database + "`").fetch().rowsUpdated().block(Duration.ofSeconds(10));
        try {
            ConnectionFactory factory = connectionFactory(database);
            DatabaseClient db = DatabaseClient.create(factory);
            db.sql("CREATE TABLE vector_outbox_sequences (aggregate_id VARCHAR(255) PRIMARY KEY,next_sequence BIGINT NOT NULL)")
                    .fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    CREATE TABLE vector_interaction_outbox (
                    event_id VARCHAR(512) PRIMARY KEY,topic VARCHAR(255) NOT NULL,record_key VARCHAR(255) NOT NULL,
                    payload LONGTEXT NOT NULL,aggregate_id VARCHAR(255) NOT NULL,sequence BIGINT NOT NULL,
                    created_at TIMESTAMP(6) NOT NULL,sent_at TIMESTAMP(6) NULL,status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
                    lease_token VARCHAR(36) NULL,lease_until TIMESTAMP(6) NULL,
                    UNIQUE KEY aggregate_sequence (aggregate_id,sequence))
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));
            db.sql("""
                    CREATE TABLE post_interaction_receipts (
                    actor_id VARCHAR(64) NOT NULL,event_id CHAR(36) NOT NULL,post_id VARCHAR(64) NOT NULL,
                    impression_id CHAR(36) NOT NULL,payload_hash CHAR(64) NOT NULL,computed_score INT NOT NULL,
                    accepted_at TIMESTAMP(6) NOT NULL,PRIMARY KEY(actor_id,event_id))
                    """).fetch().rowsUpdated().block(Duration.ofSeconds(10));
            var receipts = new PostInteractionReceiptRepository(db);
            var outboxRepository = new OutboxRepository(db);
            var transactions = TransactionalOperator.create(new R2dbcTransactionManager(factory));
            var query = mock(PostFeedQueryService.class);
            when(query.getApprovedPostById("post1")).thenReturn(Mono.just(PostDetails.builder().postId("post1").validateStatus("APPROVED").build()));
            var service = new PostInteractionService(receipts, query, new InteractionOutbox(outboxRepository, transactions), new PostInteractionScorePolicy());
            ReflectionTestUtils.setField(service, "ingestionEnabled", true);
            String eventId = UUID.randomUUID().toString();
            var request = new PostInteractionRequest("post1", true, 61, eventId, UUID.randomUUID().toString());
            var replies = Flux.range(0, 8).flatMap(i -> service.accept("actor1", request), 8).collectList().block(Duration.ofSeconds(20));
            assertThat(replies).hasSize(8);
            assertThat(replies.stream().filter(reply -> !reply.duplicate()).count()).isEqualTo(1);
            assertThat(replies).allMatch(reply -> reply.computedScore() == 3);
            assertThat(db.sql("SELECT COUNT(*) AS total FROM vector_interaction_outbox").map((row, meta) -> row.get("total", Long.class)).one()
                    .block(Duration.ofSeconds(10))).isEqualTo(1L);
            assertThat(db.sql("SELECT record_key FROM vector_interaction_outbox").map((row, meta) -> row.get("record_key", String.class)).one()
                    .block(Duration.ofSeconds(10))).isEqualTo("post1");
            assertThat(db.sql("SELECT aggregate_id FROM vector_interaction_outbox").map((row, meta) -> row.get("aggregate_id", String.class)).one()
                    .block(Duration.ofSeconds(10))).isEqualTo("actor1");

            var failingRepository = mock(OutboxRepository.class);
            when(failingRepository.append(anyString(), anyString(), anyString(), anyString(), anyString(), any()))
                    .thenReturn(Mono.error(new IllegalStateException("Outbox failure")));
            var failing = new PostInteractionService(receipts, query, new InteractionOutbox(failingRepository, transactions), new PostInteractionScorePolicy());
            ReflectionTestUtils.setField(failing, "ingestionEnabled", true);
            String secondId = UUID.randomUUID().toString();
            var second = new PostInteractionRequest("post1", true, 61, secondId, UUID.randomUUID().toString());
            StepVerifier.create(failing.accept("actor1", second)).expectErrorMessage("Outbox failure").verify(Duration.ofSeconds(10));
            StepVerifier.create(receipts.find("actor1", secondId)).verifyComplete();
        } finally {
            // Name is generated above, never read from application configuration or user input.
            admin.sql("DROP DATABASE `" + database + "`").fetch().rowsUpdated().block(Duration.ofSeconds(10));
        }
    }

    private ConnectionFactory connectionFactory(String database) {
        var options = ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.DRIVER, "mysql")
                .option(ConnectionFactoryOptions.HOST, System.getenv().getOrDefault("POPULARITY_TEST_MYSQL_HOST", "127.0.0.1"))
                .option(ConnectionFactoryOptions.PORT, Integer.parseInt(System.getenv("POPULARITY_TEST_MYSQL_PORT")))
                .option(ConnectionFactoryOptions.USER, System.getenv().getOrDefault("POPULARITY_TEST_MYSQL_USER", "root"))
                .option(ConnectionFactoryOptions.PASSWORD, System.getenv().getOrDefault("POPULARITY_TEST_MYSQL_PASSWORD", ""))
                .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(5));
        if (database != null) options.option(ConnectionFactoryOptions.DATABASE, database);
        return ConnectionFactories.get(options.build());
    }
}
