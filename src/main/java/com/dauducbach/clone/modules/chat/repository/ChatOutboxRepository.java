package com.dauducbach.clone.modules.chat.repository;

import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class ChatOutboxRepository {
    private final DatabaseClient databaseClient;
    private final ObjectMapper objectMapper;

    public Mono<Void> append(ChatEvent event) {
        return Mono.fromCallable(() -> objectMapper.writeValueAsString(event))
                .flatMap(payload -> databaseClient.sql("""
                        INSERT INTO chat_reaction_outbox (id, conversation_id, payload, created_at, available_at)
                        VALUES (:id, :conversationId, :payload, :createdAt, :createdAt)
                        """).bind("id", event.eventId()).bind("conversationId", event.conversationId())
                        .bind("payload", payload).bind("createdAt", Instant.now()).fetch().rowsUpdated()).then();
    }

    public Flux<String> candidates(Instant now) {
        return databaseClient.sql("""
                SELECT id FROM chat_reaction_outbox WHERE available_at <= :now
                AND (leased_until IS NULL OR leased_until < :now) ORDER BY created_at, id LIMIT 50
                """).bind("now", now).map((row, metadata) -> row.get("id", String.class)).all();
    }

    public Mono<LeasedEvent> lease(String id, String token, Instant now) {
        return databaseClient.sql("""
                UPDATE chat_reaction_outbox SET lease_token = :token, leased_until = :until, attempts = attempts + 1
                WHERE id = :id AND available_at <= :now AND (leased_until IS NULL OR leased_until < :now)
                """).bind("token", token).bind("until", now.plus(Duration.ofSeconds(60)))
                .bind("id", id).bind("now", now).fetch().rowsUpdated()
                .flatMap(updated -> updated == 0 ? Mono.empty() : databaseClient.sql("""
                        SELECT payload, attempts FROM chat_reaction_outbox WHERE id = :id AND lease_token = :token
                        """).bind("id", id).bind("token", token)
                        .map((row, metadata) -> new LeasedEvent(id, token, row.get("payload", String.class),
                                ((Number) row.get("attempts")).intValue())).one());
    }

    public Mono<Void> complete(LeasedEvent event) {
        return databaseClient.sql("DELETE FROM chat_reaction_outbox WHERE id = :id AND lease_token = :token")
                .bind("id", event.id()).bind("token", event.token()).fetch().rowsUpdated().then();
    }

    public Mono<Void> retry(LeasedEvent event, Instant nextAttempt) {
        return databaseClient.sql("""
                UPDATE chat_reaction_outbox SET lease_token = NULL, leased_until = NULL, available_at = :nextAttempt
                WHERE id = :id AND lease_token = :token
                """).bind("id", event.id()).bind("token", event.token()).bind("nextAttempt", nextAttempt)
                .fetch().rowsUpdated().then();
    }

    public record LeasedEvent(String id, String token, String payload, int attempts) {}
}
