package com.dauducbach.clone.modules.chat;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.constant.ReactionType;
import com.dauducbach.clone.modules.chat.dto.request.*;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.repository.*;
import com.dauducbach.clone.modules.chat.service.*;
import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetRegistry;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Real MySQL, R2DBC mapping, repository SQL and transactions. Only its generated database is modified. */
@EnabledIfEnvironmentVariable(named = "CHAT_TEST_MYSQL_PORT", matches = "[0-9]+")
class ChatDatabaseIntegrationTest {
    private static final Duration TIMEOUT = Duration.ofSeconds(20);
    private String database;
    private DatabaseClient admin, db;
    private R2dbcEntityTemplate template;
    private ConversationRepository conversations;
    private ConversationMemberRepository members;
    private ChatMessageRepository messages;
    private ChatReadRepository reads;
    private ChatOutboxRepository outbox;
    private ChatAccessService access;
    private TransactionalOperator tx;
    private MediaAssetRegistry media;
    private final ChatResponseMapper mapper = new ChatResponseMapper();
    private SendMessageService sender;

    @BeforeEach
    void createIsolatedDatabase() throws Exception {
        database = "test_chat_" + UUID.randomUUID().toString().replace("-", "");
        admin = DatabaseClient.create(factory(null));
        admin.sql("CREATE DATABASE `" + database + "`").fetch().rowsUpdated().block(TIMEOUT);
        ConnectionFactory connectionFactory = factory(database);
        db = DatabaseClient.create(connectionFactory);
        template = new R2dbcEntityTemplate(connectionFactory);
        var repositories = new R2dbcRepositoryFactory(template);
        conversations = repositories.getRepository(ConversationRepository.class);
        members = repositories.getRepository(ConversationMemberRepository.class);
        messages = repositories.getRepository(ChatMessageRepository.class);
        reads = new ChatReadRepository(db);
        outbox = new ChatOutboxRepository(db, new ObjectMapper().findAndRegisterModules());
        access = new ChatAccessService(members);
        tx = TransactionalOperator.create(new R2dbcTransactionManager(connectionFactory));
        media = mock(MediaAssetRegistry.class);
        // Extract CREATE statements only: legacy base file also contains unrelated rollout DDL/seeds.
        var blocks = Pattern.compile("(?s)CREATE TABLE IF NOT EXISTS .*?ENGINE=InnoDB;")
                .matcher(script("chat_schema.sql"));
        while (blocks.find()) sql(blocks.group());
        sql("CREATE TABLE media (asset_id VARCHAR(255) PRIMARY KEY, owner_id VARCHAR(36), owner_type VARCHAR(32)) ENGINE=InnoDB");
        apply("chat_reactions_schema.sql");
        apply("chat_message_actions_schema.sql");
        sql("ALTER TABLE messages DROP CHECK chk_message_type");
        sql("ALTER TABLE messages ADD CONSTRAINT chk_message_type CHECK (message_type IN ('TEXT','IMAGE','VIDEO','FILE','AUDIO','SYSTEM','STORY_REPLY'))");
        seedConversation("c");
        sender = sender(outbox);
    }

    @AfterEach
    void removeOnlyGeneratedDatabase() {
        if (admin != null && database != null && database.matches("test_chat_[a-f0-9]{32}")) {
            admin.sql("DROP DATABASE IF EXISTS `" + database + "`").fetch().rowsUpdated().block(TIMEOUT);
        }
    }

    @Test
    void missingPayloadHashReproduces1323AndMigrationPreservesExistingMessages() throws Exception {
        var first = send(text("legacy"));
        assertThat(missingSchemaRequirements()).isEmpty();
        sql("ALTER TABLE messages DROP COLUMN client_payload_hash");
        assertThat(missingSchemaRequirements()).containsExactly("messages.client_payload_hash");
        var request = text("new message");
        StepVerifier.create(sender.sendMessage("actor", "c", request))
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode()).isEqualTo(ErrorCode.CHAT_MESSAGE_CREATE_FAILED);
                    assertThat(root(error).getMessage()).contains("client_payload_hash");
                }).verify(TIMEOUT);
        assertThat(count("messages")).isEqualTo(1);
        apply("chat_message_idempotency_hash.sql");
        assertThat(missingSchemaRequirements()).isEmpty();
        assertThat(messages.findById(first.id()).block(TIMEOUT).getClientPayloadHash()).isNull();
        var accepted = send(request);
        assertThat(accepted.messageSeq()).isEqualTo(2);
        assertThat(messages.findById(accepted.id()).block(TIMEOUT).getClientPayloadHash()).hasSize(64);
        assertThat(send(request).id()).isEqualTo(accepted.id());
        assertThat(count("messages")).isEqualTo(2);
    }

    @Test
    void concurrentRetriesCommitOneMessageSequenceAndDurableIntent() {
        var request = text("hello");
        var results = Flux.range(0, 8).flatMap(i -> sender.sendMessage("actor", "c", request), 8)
                .collectList().block(TIMEOUT);
        assertThat(results).hasSize(8).allMatch(r -> r.id().equals(results.getFirst().id()));
        assertThat(count("messages")).isEqualTo(1);
        assertThat(count("chat_reaction_outbox")).isEqualTo(1);
        assertThat(conversations.findById("c").block(TIMEOUT).getLastMessageSeq()).isEqualTo(1);
        var event = db.sql("SELECT payload FROM chat_reaction_outbox")
                .map((r, m) -> r.get("payload", String.class)).one().block(TIMEOUT);
        assertThat(event).contains("MESSAGE_CREATED", "peer").doesNotContain("clientPayloadHash");
        var changed = new SendMessageRequest(request.clientMessageId(), MessageType.TEXT, "different", null, null, "peer", null, null);
        expectCode(sender.sendMessage("actor", "c", changed), ErrorCode.CHAT_MESSAGE_IDEMPOTENCY_CONFLICT);
        seedConversation("other");
        expectCode(sender.sendMessage("actor", "other", request), ErrorCode.CHAT_MESSAGE_IDEMPOTENCY_CONFLICT);
    }

    @Test
    void distinctConcurrentSendsProduceContiguousOrderedHistory() {
        Flux.range(0, 8).flatMap(i -> sender.sendMessage("actor", "c", text("message " + i)), 8)
                .collectList().block(TIMEOUT);
        var rows = reads.findAfterSequence("c", 1, 0, 100).collectList().block(TIMEOUT);
        assertThat(rows).extracting(row -> row.getMessageSeq()).containsExactly(1L,2L,3L,4L,5L,6L,7L,8L);
        assertThat(count("chat_reaction_outbox")).isEqualTo(8);
    }

    @Test
    void outboxFailureRollsBackMessageAndSummaryAndRetryDoesNotSkipSequence() {
        var failedOutbox = mock(ChatOutboxRepository.class);
        when(failedOutbox.append(any())).thenReturn(Mono.error(new IllegalStateException("outbox unavailable")));
        var request = text("retry me");
        expectCode(sender(failedOutbox).sendMessage("actor", "c", request), ErrorCode.CHAT_MESSAGE_CREATE_FAILED);
        assertThat(count("messages")).isZero();
        assertThat(conversations.findById("c").block(TIMEOUT).getLastMessageSeq()).isZero();
        assertThat(send(request).messageSeq()).isEqualTo(1);
    }

    @ParameterizedTest
    @EnumSource(value = MessageType.class, names = {"IMAGE", "AUDIO", "VIDEO", "FILE"})
    void mediaSendUsesRealJsonMappingAndRollsBackIfRegistrationFails(MessageType type) {
        String mime = switch (type) { case IMAGE -> "image/png"; case AUDIO -> "audio/webm"; case VIDEO -> "video/mp4"; default -> "application/pdf"; };
        var asset = new MediaAssetView("asset", "public", 100, 80, "format", "raw", 500,
                "https://example.com/upload", "https://example.com/secure", null, OwnerType.CHAT_MESSAGE,
                "1", "1", "name", Instant.now(), Instant.now());
        when(media.fetchRemoteAsset("public")).thenReturn(Mono.just(asset));
        when(media.registerFetchedAsset(any(), anyString(), eq(OwnerType.CHAT_MESSAGE)))
                .thenReturn(Mono.error(new IllegalStateException("registration failed")));
        var request = new SendMessageRequest(UUID.randomUUID().toString(), type, null,
                new MediaMetadataRequest("https://example.com/upload", "public", mime, 500L, "file", 100, 80, 1000L),
                null, "peer", null, null);
        expectCode(sender.sendMessage("actor", "c", request), ErrorCode.CHAT_MESSAGE_CREATE_FAILED);
        assertThat(count("messages")).isZero();
        assertThat(count("chat_reaction_outbox")).isZero();
        when(media.registerFetchedAsset(any(), anyString(), eq(OwnerType.CHAT_MESSAGE))).thenReturn(Mono.just(asset));
        var accepted = send(request);
        assertThat(accepted.metadata().url()).isEqualTo(asset.secureUrl());
        assertThat(accepted.messageType()).isEqualTo(type);
        assertThat(send(request).id()).isEqualTo(accepted.id());
        verify(media, times(2)).fetchRemoteAsset("public"); // Failed attempt + retry; accepted retry reuses row.
    }

    @Test
    void replyAndStoryMetadataAreHydratedAndUnauthorizedSendsAreRejected() {
        var first = send(text("original"));
        var reply = send(new SendMessageRequest(UUID.randomUUID().toString(), MessageType.TEXT, "reply", null,
                first.messageSeq(), "peer", null, null));
        assertThat(reply.reply().content()).isEqualTo("original");
        var story = send(new SendMessageRequest(UUID.randomUUID().toString(), MessageType.STORY_REPLY, "story reply", null,
                null, "peer", null, new StoryContextRequest("story", "peer", "IMAGE", 0L, Instant.now().plusSeconds(300))));
        assertThat(story.storyContext().storyId()).isEqualTo("story");
        expectCode(sender.sendMessage("outsider", "c", text("no access")), ErrorCode.CONVERSATION_FORBIDDEN);
        assertThat(count("messages")).isEqualTo(3);
    }

    @Test
    void cannotReplyToMessagesBeforeJoinOrClearedHistory() {
        var original = send(text("hidden original"));
        var reply = new SendMessageRequest(UUID.randomUUID().toString(), MessageType.TEXT, "reply", null,
                original.messageSeq(), "peer", null, null);
        sql("UPDATE conversation_members SET joined_seq=2 WHERE user_id='actor'");
        expectCode(sender.sendMessage("actor", "c", reply), ErrorCode.CHAT_MESSAGE_REPLY_INVALID);
        sql("UPDATE conversation_members SET joined_seq=1,last_deleted_message_seq=1 WHERE user_id='actor'");
        expectCode(sender.sendMessage("actor", "c", reply), ErrorCode.CHAT_MESSAGE_REPLY_INVALID);
        assertThat(count("messages")).isEqualTo(1);
        assertThat(count("chat_reaction_outbox")).isEqualTo(1);
    }

    @Test
    void acceptedRetryCannotRestoreClearedMessageOrItsReplyQuote() {
        var originalRequest = text("hidden original");
        var original = send(originalRequest);
        var replyRequest = new SendMessageRequest(UUID.randomUUID().toString(), MessageType.TEXT, "visible reply", null,
                original.messageSeq(), "peer", null, null);
        send(replyRequest);
        sql("UPDATE conversation_members SET last_deleted_message_seq=1 WHERE user_id='actor'");
        var hiddenQuote = send(replyRequest).reply();
        assertThat(hiddenQuote.deleted()).isTrue();
        assertThat(hiddenQuote.content()).isNull();
        assertThat(hiddenQuote.metadata()).isNull();
        assertThat(hiddenQuote.senderId()).isNull();
        expectCode(sender.sendMessage("actor", "c", originalRequest), ErrorCode.CONVERSATION_FORBIDDEN);
        assertThat(count("messages")).isEqualTo(2);
    }

    @Test
    void realtimeRechecksActualMembershipAndRedactsQuotesPerRecipient() throws Exception {
        var original = send(text("private old quote"));
        var reply = send(new SendMessageRequest(UUID.randomUUID().toString(), MessageType.TEXT, "visible reply", null,
                original.messageSeq(), "peer", null, null));
        sql("UPDATE conversation_members SET last_deleted_message_seq=1 WHERE user_id='peer'");
        var json = new ObjectMapper().findAndRegisterModules();
        var registry = mock(ChatSessionRegistry.class);
        var dispatcher = new ChatRealtimeLocalDispatcher(json, registry, new MessageReactionRepository(db), members);
        var event = com.dauducbach.clone.modules.chat.publicapi.ChatEvent.messageCreated(reply, List.of("actor", "peer", "outsider"));
        var payload = json.writeValueAsString(event);
        dispatcher.dispatch(payload).block(TIMEOUT);
        verify(registry).sendToUser(eq("peer"), argThat(value -> value.contains("visible reply") && !value.contains("private old quote")));
        verify(registry).sendToUser("actor", payload);
        verifyNoMoreInteractions(registry);
        clearInvocations(registry);
        sql("UPDATE conversation_members SET member_status='LEFT' WHERE user_id='peer'");
        sql("UPDATE conversation_members SET joined_seq=3 WHERE user_id='actor'");
        dispatcher.dispatch(payload).block(TIMEOUT);
        verifyNoInteractions(registry);
    }

    @Test
    void failedBrokerDeliveryRetainsCommittedMessageAndRetriesDurableOutbox() {
        var accepted = send(text("durable message"));
        var broker = mock(ChatEventPublisher.class);
        when(broker.publish(any())).thenReturn(Mono.error(new IllegalStateException("broker unavailable")));
        var publisher = new ChatOutboxPublisher(outbox, broker, new MessageReactionRepository(db),
                new ObjectMapper().findAndRegisterModules(), messages, reads, mapper);
        publisher.drain().block(TIMEOUT);
        assertThat(count("messages")).isEqualTo(1);
        assertThat(count("chat_reaction_outbox")).isEqualTo(1);
        assertThat(outbox.candidates(Instant.now()).collectList().block(TIMEOUT)).isEmpty();
        sql("UPDATE chat_reaction_outbox SET available_at=NOW(3)");
        when(broker.publish(any())).thenReturn(Mono.empty());
        publisher.drain().block(TIMEOUT);
        assertThat(count("chat_reaction_outbox")).isZero();
        assertThat(messages.findById(accepted.id()).block(TIMEOUT).getContent()).isEqualTo("durable message");
        verify(broker, times(2)).publish(any());
    }

    @Test
    void readCursorsStayMonotonicAndRollBackWhenOutboxCannotPersist() {
        send(text("first"));
        send(text("second"));
        var failedQueue = mock(ChatOutboxRepository.class);
        when(failedQueue.append(any())).thenReturn(Mono.error(new IllegalStateException("outbox unavailable")));
        expectCode(new ChatCursorService(access, members, conversations, failedQueue, tx).markRead("peer", "c", 2),
                ErrorCode.CHAT_CURSOR_UPDATE_FAILED);
        assertThat(members.findActive("c", "peer").block(TIMEOUT).getLastReadSeq()).isZero();
        var cursor = new ChatCursorService(access, members, conversations, outbox, tx);
        cursor.markRead("peer", "c", 2).block(TIMEOUT);
        assertThat(cursor.markRead("peer", "c", 1).block(TIMEOUT).readSeq()).isEqualTo(2);
        assertThat(cursor.markDelivered("peer", "c", 1).block(TIMEOUT).deliveredSeq()).isEqualTo(2);
        expectCode(cursor.markRead("peer", "c", 3), ErrorCode.CHAT_MESSAGE_SEQUENCE_INVALID);
        var stored = members.findActive("c", "peer").block(TIMEOUT);
        assertThat(stored.getLastReadSeq()).isEqualTo(2);
        assertThat(stored.getLastDeliveredSeq()).isEqualTo(2);
    }

    @Test
    void reactionsPinsForwardRecallAndMonotonicCursorsUseActualRepositorySql() {
        var original = send(text("original"));
        var reactionRows = new MessageReactionRepository(db);
        var reactions = new MessageReactionService(reactionRows, conversations, members, access, outbox, tx);
        reactions.set("peer", "c", original.id(), ReactionType.HEART).block(TIMEOUT);
        assertThat(reactions.getSnapshots("peer", "c", List.of(original.id())).block(TIMEOUT).getFirst().myReaction()).isEqualTo(ReactionType.HEART);
        reactions.remove("peer", "c", original.id()).block(TIMEOUT);
        var actions = new ChatMessageActionsRepository(db);
        var actionAccess = new ChatMessageAccess(conversations, members, actions);
        var pins = new ChatPinService(actionAccess, actions, reads, outbox, members, mapper, tx, 5);
        assertThat(pins.put("actor", "c", original.id()).block(TIMEOUT).items()).hasSize(1);
        var cursor = new ChatCursorService(access, members, conversations, outbox, tx);
        assertThat(cursor.markRead("peer", "c", 1).block(TIMEOUT).readSeq()).isEqualTo(1);
        seedConversation("other");
        var forward = new ChatForwardService(actionAccess, actions, messages,
                new ChatMessageWriter(template, conversations, outbox, mapper), members, mapper, tx);
        var request = new ForwardMessageRequest("c", original.id(), UUID.randomUUID().toString());
        var copy = forward.forward("actor", "other", request).block(TIMEOUT);
        assertThat(copy.forwarded()).isTrue();
        assertThat(forward.forward("actor", "other", request).block(TIMEOUT).id()).isEqualTo(copy.id());
        var recall = new ChatRecallService(actionAccess, actions, outbox, reactionRows, mapper, tx);
        assertThat(recall.recall("actor", "c", original.id()).block(TIMEOUT).content()).isNull();
        assertThat(pins.get("actor", "c").block(TIMEOUT).items()).isEmpty();
        assertThat(messages.findById(copy.id()).block(TIMEOUT).getContent()).isEqualTo("original");
    }

    private SendMessageService sender(ChatOutboxRepository queue) {
        return new SendMessageService(messages, reads, conversations, members, access,
                new ChatMessageValidator(new MediaPolicyProperties()), mapper, tx,
                new ChatMessageWriter(template, conversations, queue, mapper), media);
    }

    private SendMessageRequest text(String content) {
        return new SendMessageRequest(UUID.randomUUID().toString(), MessageType.TEXT, content, null, null, "peer", null, null);
    }
    private ChatMessageResponse send(SendMessageRequest request) { return sender.sendMessage("actor", "c", request).block(TIMEOUT); }
    private void expectCode(Mono<?> work, ErrorCode code) {
        StepVerifier.create(work).expectErrorSatisfies(error -> assertThat(error)
                .isInstanceOfSatisfying(AppException.class, app -> assertThat(app.getErrorCode()).isEqualTo(code))).verify(TIMEOUT);
    }
    private Throwable root(Throwable error) { while (error.getCause() != null) error = error.getCause(); return error; }
    private long count(String table) { return db.sql("SELECT COUNT(*) AS n FROM " + table).map((r, m) -> r.get("n", Long.class)).one().block(TIMEOUT); }
    private void seedConversation(String id) {
        db.sql("INSERT INTO conversations(id,conversation_type,created_by,created_at,updated_at) VALUES (:id,'DIRECT','actor',NOW(3),NOW(3))")
                .bind("id", id).fetch().rowsUpdated().block(TIMEOUT);
        for (String user : List.of("actor", "peer")) {
            db.sql("INSERT INTO conversation_members(id,conversation_id,user_id,member_role,member_status,joined_seq,last_delivered_seq,last_read_seq,joined_at) VALUES (:key,:id,:user,'USER','ACTIVE',1,0,0,NOW(3))")
                    .bind("key", UUID.randomUUID().toString()).bind("id", id).bind("user", user).fetch().rowsUpdated().block(TIMEOUT);
        }
    }
    private String script(String name) throws Exception { return Files.readString(Path.of("src/main/resources/db/manual", name)); }
    private List<String> missingSchemaRequirements() throws Exception {
        return db.sql(script("chat_schema_preflight.sql"))
                .map((row, metadata) -> row.get("table_name", String.class) + "." + row.get("missing_column", String.class))
                .all().collectList().block(TIMEOUT);
    }
    private void apply(String name) throws Exception {
        String ddl = script(name).replaceAll("(?m)^--[^\\r\\n]*", "");
        for (String statement : ddl.split(";")) if (!statement.isBlank()) sql(statement);
    }
    private void sql(String ddl) { db.sql(ddl).fetch().rowsUpdated().block(TIMEOUT); }
    private ConnectionFactory factory(String schema) {
        var options = ConnectionFactoryOptions.builder().option(ConnectionFactoryOptions.DRIVER, "mysql")
                .option(ConnectionFactoryOptions.HOST, System.getenv().getOrDefault("CHAT_TEST_MYSQL_HOST", "127.0.0.1"))
                .option(ConnectionFactoryOptions.PORT, Integer.parseInt(System.getenv("CHAT_TEST_MYSQL_PORT")))
                .option(ConnectionFactoryOptions.USER, System.getenv().getOrDefault("CHAT_TEST_MYSQL_USER", "root"))
                .option(ConnectionFactoryOptions.PASSWORD, System.getenv().getOrDefault("CHAT_TEST_MYSQL_PASSWORD", ""))
                .option(ConnectionFactoryOptions.CONNECT_TIMEOUT, Duration.ofSeconds(5));
        if (schema != null) options.option(ConnectionFactoryOptions.DATABASE, schema);
        return ConnectionFactories.get(options.build());
    }
}
