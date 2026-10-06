package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.constant.ReactionType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.RowsFetchSpec;
import reactor.core.publisher.Flux;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.concurrent.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MessageReactionRepositoryTest {
    Connection db;
    @BeforeEach void setup() throws Exception {
        db = DriverManager.getConnection("jdbc:h2:mem:reactions;MODE=MySQL;DB_CLOSE_DELAY=-1");
        db.createStatement().execute("DROP ALL OBJECTS");
        db.createStatement().execute("CREATE TABLE messages(id VARCHAR(36) PRIMARY KEY, conversation_id VARCHAR(36), message_seq BIGINT, message_type VARCHAR(16), deleted_at TIMESTAMP)");
        db.createStatement().execute("CREATE TABLE conversation_members(conversation_id VARCHAR(36), user_id VARCHAR(64), member_status VARCHAR(16), joined_seq BIGINT, last_deleted_message_seq BIGINT)");
        db.createStatement().execute("CREATE TABLE user_details(user_id VARCHAR(64), username VARCHAR(64), full_name VARCHAR(64))");
        db.createStatement().execute("CREATE TABLE media(asset_id VARCHAR(255), owner_id VARCHAR(64), owner_type VARCHAR(32), secure_url VARCHAR(255), url VARCHAR(255), created_at TIMESTAMP)");
        String migration = Files.readString(Path.of("src/main/resources/db/manual/chat_reactions_schema.sql"))
                .replaceAll("(?m)^--.*$", "").replace(" ENGINE=InnoDB", "")
                .replace("KEY idx_message_reaction_list (message_id, reaction, user_id),", "")
                .replace(",\n    KEY idx_chat_reaction_outbox_ready (available_at, leased_until, created_at)", "");
        for (String sql : migration.split(";")) if (!sql.isBlank()) db.createStatement().execute(sql);
        db.createStatement().execute("INSERT INTO messages VALUES('m', 'c', 5, 'TEXT', NULL, 0), ('hidden', 'c', 1, 'TEXT', NULL, 0)");
        db.createStatement().execute("INSERT INTO conversation_members VALUES('c','me','ACTIVE',2,NULL)");
    }
    @AfterEach void close() throws Exception { db.close(); }

    String bound(String sql) {
        return sql.replace(":conversationId", "'c'").replace(":messageIds", "'m','hidden'")
                .replace(":messageId", "'m'").replace(":viewerId", "'me'").replace(":visibleFrom", "2")
                .replace(":userId", "'me'").replace(":reaction", "'HEART'")
                .replace(":reactedAt", "TIMESTAMP '2026-10-06 10:00:00'").replace(":cursor", "''").replace(":limit", "2");
    }
    @Test void realSqlUpsertCountsAndSnapshotRespectViewerVisibility() throws Exception {
        db.createStatement().execute(bound(MessageReactionRepository.SET_SQL));
        db.createStatement().execute(bound(MessageReactionRepository.SET_SQL).replace("'HEART'", "'ANGRY'"));
        db.createStatement().execute("UPDATE messages SET reaction_version=2 WHERE id='m'");
        db.createStatement().execute("INSERT INTO message_reactions VALUES('m','other','HEART',TIMESTAMP '2026-10-06 10:00:00')");
        try (ResultSet rows = db.createStatement().executeQuery(bound(MessageReactionRepository.SNAPSHOTS_SQL))) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString("id")).isEqualTo("m");
            assertThat(rows.getString("my_reaction")).isEqualTo("ANGRY");
            assertThat(rows.getLong("heart_count")).isEqualTo(1);
            assertThat(rows.getLong("angry_count")).isEqualTo(1);
            assertThat(rows.getLong("reaction_version")).isEqualTo(2);
            assertThat(rows.next()).isFalse();
        }
        db.createStatement().execute("UPDATE conversation_members SET member_status='REMOVED'");
        try (ResultSet rows = db.createStatement().executeQuery(bound(MessageReactionRepository.SNAPSHOTS_SQL))) {
            assertThat(rows.next()).isFalse();
        }
    }

    @Test void migrationRejectsDuplicateActorAndUnsupportedCode() throws Exception {
        db.createStatement().execute(bound(MessageReactionRepository.SET_SQL));
        assertThatThrownBy(() -> db.createStatement().execute("INSERT INTO message_reactions VALUES('m','me','HEART',CURRENT_TIMESTAMP)"))
                .isInstanceOf(SQLException.class);
        assertThatThrownBy(() -> db.createStatement().execute("INSERT INTO message_reactions VALUES('m','other','BOGUS',CURRENT_TIMESTAMP)"))
                .isInstanceOf(SQLException.class);
    }

    @Test void reactorQueryUsesExistingProfileSchemaAndStableKeyset() throws Exception {
        db.createStatement().execute("INSERT INTO message_reactions VALUES('m','a','HEART',CURRENT_TIMESTAMP),('m','b','ANGRY',CURRENT_TIMESTAMP)");
        db.createStatement().execute("INSERT INTO user_details VALUES('a','alice','Alice')");
        DatabaseClient client = mock(DatabaseClient.class);
        DatabaseClient.GenericExecuteSpec spec = mock(DatabaseClient.GenericExecuteSpec.class);
        RowsFetchSpec rows = mock(RowsFetchSpec.class);
        AtomicReference<String> capturedSql = new AtomicReference<>();
        when(client.sql(anyString())).thenAnswer(i -> { capturedSql.set(i.getArgument(0)); return spec; });
        when(spec.bind(anyString(), any())).thenReturn(spec);
        when(spec.map(any(BiFunction.class))).thenReturn(rows);
        when(rows.all()).thenReturn(Flux.empty());
        new MessageReactionRepository(client).list("m", ReactionType.HEART, "", 2);
        try (ResultSet result = db.createStatement().executeQuery(bound(capturedSql.get()))) {
            assertThat(result.next()).isTrue();
            assertThat(result.getString("display_name")).isEqualTo("Alice");
            assertThat(result.getString("reaction")).isEqualTo("HEART");
            assertThat(result.next()).isFalse();
        }
    }

    @Test void rollbackRestoresReactionRevisionAndOutboxAtomically() throws Exception {
        db.setAutoCommit(false);
        db.createStatement().execute(bound(MessageReactionRepository.SET_SQL));
        db.createStatement().execute("UPDATE messages SET reaction_version=reaction_version+1 WHERE id='m'");
        db.createStatement().execute("INSERT INTO chat_reaction_outbox(id,conversation_id,payload,created_at,available_at) VALUES('event','c','{}',CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)");
        db.rollback();
        try (ResultSet result = db.createStatement().executeQuery("SELECT (SELECT COUNT(*) FROM message_reactions) AS reactions,(SELECT COUNT(*) FROM chat_reaction_outbox) AS events,reaction_version FROM messages WHERE id='m'")) {
            result.next();
            assertThat(result.getInt("reactions")).isZero();
            assertThat(result.getInt("events")).isZero();
            assertThat(result.getLong("reaction_version")).isZero();
        }
    }

    @Test void competingTransactionsSerializeAndKeepOneActorRowWithMonotonicRevision() throws Exception {
        db.createStatement().execute("CREATE TABLE conversations(id VARCHAR(36) PRIMARY KEY)");
        db.createStatement().execute("INSERT INTO conversations VALUES('c')");
        String lock = ConversationRepository.class.getMethod("findByIdForUpdate", String.class)
                .getAnnotation(org.springframework.data.r2dbc.repository.Query.class).value()
                .replace(":conversationId", "'c'");
        db.setAutoCommit(false);
        db.createStatement().executeQuery(lock).close();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch tryingLock = new CountDownLatch(1);
        try {
            Future<?> competing = executor.submit(() -> {
                try (Connection other = DriverManager.getConnection("jdbc:h2:mem:reactions;MODE=MySQL;DB_CLOSE_DELAY=-1")) {
                    other.setAutoCommit(false);
                    tryingLock.countDown();
                    other.createStatement().executeQuery(lock).close();
                    other.createStatement().execute(bound(MessageReactionRepository.SET_SQL).replace("'HEART'", "'ANGRY'"));
                    other.createStatement().execute("UPDATE messages SET reaction_version=reaction_version+1 WHERE id='m'");
                    other.commit();
                } catch (SQLException error) { throw new RuntimeException(error); }
            });
            assertThat(tryingLock.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> competing.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            db.createStatement().execute(bound(MessageReactionRepository.SET_SQL));
            db.createStatement().execute("UPDATE messages SET reaction_version=reaction_version+1 WHERE id='m'");
            db.commit();
            competing.get(3, TimeUnit.SECONDS);
            try (ResultSet result = db.createStatement().executeQuery(bound(MessageReactionRepository.SNAPSHOTS_SQL))) {
                result.next();
                assertThat(result.getLong("reaction_version")).isEqualTo(2);
                assertThat(result.getString("my_reaction")).isEqualTo("ANGRY");
                assertThat(result.getLong("angry_count")).isEqualTo(1);
                assertThat(result.getLong("heart_count")).isZero();
            }
            assertThat(db.createStatement().executeUpdate("DELETE FROM message_reactions WHERE message_id='m' AND user_id='me'"))
                    .isEqualTo(1);
            db.createStatement().execute("UPDATE messages SET reaction_version=reaction_version+1 WHERE id='m'");
            assertThat(db.createStatement().executeUpdate("DELETE FROM message_reactions WHERE message_id='m' AND user_id='me'"))
                    .isZero();
            db.commit();
            try (ResultSet result = db.createStatement().executeQuery(bound(MessageReactionRepository.SNAPSHOTS_SQL))) {
                result.next();
                assertThat(result.getLong("reaction_version")).isEqualTo(3);
                assertThat(result.getString("my_reaction")).isNull();
                assertThat(result.getLong("angry_count")).isZero();
            }
        } finally { db.rollback(); executor.shutdownNow(); }
    }

    @Test void realOutboxLeaseSqlHasSingleOwnerAndRejectsExpiredOwnersCompletion() throws Exception {
        db.createStatement().execute("INSERT INTO chat_reaction_outbox(id,conversation_id,payload,created_at,available_at) VALUES('event','c','{}',TIMESTAMP '2026-10-06 10:00:00',TIMESTAMP '2026-10-06 10:00:00')");
        DatabaseClient client = mock(DatabaseClient.class);
        DatabaseClient.GenericExecuteSpec spec = mock(DatabaseClient.GenericExecuteSpec.class);
        org.springframework.r2dbc.core.FetchSpec fetch = mock(org.springframework.r2dbc.core.FetchSpec.class);
        AtomicReference<String> capturedSql = new AtomicReference<>();
        when(client.sql(anyString())).thenAnswer(i -> { capturedSql.set(i.getArgument(0)); return spec; });
        when(spec.bind(anyString(), any())).thenReturn(spec);
        when(spec.fetch()).thenReturn(fetch);
        when(fetch.rowsUpdated()).thenReturn(reactor.core.publisher.Mono.just(0L));
        var repository = new ChatReactionOutboxRepository(client, new com.fasterxml.jackson.databind.ObjectMapper());
        repository.lease("event", "owner-a", Instant.parse("2026-10-06T10:00:00Z")).block();
        String leaseSql = capturedSql.get().replace(":id", "'event'").replace(":token", "'owner-a'")
                .replace(":until", "TIMESTAMP '2026-10-06 10:01:00'")
                .replace(":now", "TIMESTAMP '2026-10-06 10:00:00'");
        assertThat(db.createStatement().executeUpdate(leaseSql)).isEqualTo(1);
        assertThat(db.createStatement().executeUpdate(leaseSql.replace("owner-a", "owner-b"))).isZero();
        assertThat(db.createStatement().executeUpdate(leaseSql.replace("owner-a", "owner-b")
                .replace("10:00:00", "10:02:00").replace("10:01:00", "10:03:00"))).isEqualTo(1);
        repository.complete(new ChatReactionOutboxRepository.LeasedEvent("event", "owner-a", "{}", 1)).block();
        String completeSql = capturedSql.get().replace(":id", "'event'").replace(":token", "'owner-a'");
        assertThat(db.createStatement().executeUpdate(completeSql)).isZero();
        assertThat(db.createStatement().executeUpdate(completeSql.replace("owner-a", "owner-b"))).isEqualTo(1);
    }
}
