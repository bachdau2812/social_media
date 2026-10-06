package com.dauducbach.clone.infrastructure.outbox;

import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class OutboxRepository {
    private final DatabaseClient db;

    // Caller owns a transaction. This row lock serializes allocation and commit for one user.
    public Mono<Void> append(String eventId, String topic, String key, String payload, Instant occurredAt) {
        return append(eventId, topic, key, key, payload, occurredAt);
    }

    public Mono<Void> append(String eventId, String topic, String aggregateId, String key, String payload, Instant occurredAt) {
        return db.sql("""
                INSERT INTO vector_outbox_sequences (aggregate_id, next_sequence) VALUES (:user, 1)
                ON DUPLICATE KEY UPDATE next_sequence = next_sequence + 1
                """).bind("user", aggregateId).fetch().rowsUpdated()
                .then(db.sql("SELECT next_sequence FROM vector_outbox_sequences WHERE aggregate_id = :user")
                        .bind("user", aggregateId).map((row, meta) -> row.get("next_sequence", Long.class)).one())
                .flatMap(sequence -> db.sql("""
                        INSERT INTO vector_interaction_outbox
                        (event_id, topic, record_key, payload, aggregate_id, sequence, created_at, status)
                        VALUES (:id, :topic, :key, :payload, :aggregate, :sequence, :created, 'PENDING')
                        """).bind("id", eventId).bind("topic", topic).bind("key", key)
                        .bind("aggregate", aggregateId).bind("payload", payload).bind("sequence", sequence).bind("created", occurredAt)
                        .fetch().rowsUpdated()).then();
    }

    public Flux<OutboxEvent> pendingHeads(int limit) {
        return db.sql("""
                SELECT o.* FROM vector_interaction_outbox o
                WHERE o.status <> 'SENT' AND (o.lease_until IS NULL OR o.lease_until < CURRENT_TIMESTAMP(6))
                AND NOT EXISTS (SELECT 1 FROM vector_interaction_outbox p
                    WHERE p.aggregate_id = o.aggregate_id AND p.sequence < o.sequence AND p.status <> 'SENT')
                ORDER BY o.created_at, o.sequence LIMIT :limit
                """).bind("limit", limit).map((row, meta) -> new OutboxEvent(
                        row.get("event_id", String.class), row.get("topic", String.class),
                        row.get("record_key", String.class), row.get("payload", String.class),
                        row.get("aggregate_id", String.class), row.get("sequence", Long.class),
                        row.get("created_at", Instant.class))).all();
    }

    public Mono<Boolean> claim(String id, String token, long leaseSeconds) {
        // Multi-table UPDATE avoids MySQL's target-table subquery restriction.
        return db.sql("""
                UPDATE vector_interaction_outbox o
                LEFT JOIN vector_interaction_outbox p ON p.aggregate_id = o.aggregate_id
                    AND p.sequence < o.sequence AND p.status <> 'SENT'
                SET o.status = 'SENDING', o.lease_token = :token,
                    o.lease_until = TIMESTAMPADD(SECOND, :seconds, CURRENT_TIMESTAMP(6))
                WHERE o.event_id = :id AND o.status <> 'SENT' AND p.event_id IS NULL
                    AND (o.lease_until IS NULL OR o.lease_until < CURRENT_TIMESTAMP(6))
                """).bind("token", token).bind("seconds", leaseSeconds).bind("id", id)
                .fetch().rowsUpdated().map(count -> count == 1);
    }

    public Mono<Void> markSent(String id, String token) {
        return db.sql("""
                UPDATE vector_interaction_outbox SET status = 'SENT', sent_at = CURRENT_TIMESTAMP(6),
                    lease_token = NULL, lease_until = NULL
                WHERE event_id = :id AND lease_token = :token AND lease_until > CURRENT_TIMESTAMP(6)
                """).bind("id", id).bind("token", token).fetch().rowsUpdated()
                .flatMap(count -> count == 1 ? Mono.empty() : Mono.error(new IllegalStateException("Outbox lease lost before ACK")));
    }

    public Mono<Void> release(String id, String token) {
        return db.sql("""
                UPDATE vector_interaction_outbox SET status = 'PENDING', lease_token = NULL, lease_until = NULL
                WHERE event_id = :id AND lease_token = :token
                """).bind("id", id).bind("token", token).fetch().rowsUpdated().then();
    }
}
