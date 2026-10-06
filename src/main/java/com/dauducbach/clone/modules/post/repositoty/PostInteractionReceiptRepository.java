package com.dauducbach.clone.modules.post.repositoty;

import com.dauducbach.clone.modules.post.entity.PostInteractionReceipt;
import lombok.RequiredArgsConstructor;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Repository
@RequiredArgsConstructor
public class PostInteractionReceiptRepository {
    private final DatabaseClient db;

    public Mono<PostInteractionReceipt> find(String actor, String event) {
        return db.sql("SELECT * FROM post_interaction_receipts WHERE actor_id = :actor AND event_id = :event")
                .bind("actor", actor).bind("event", event).map((row, meta) -> new PostInteractionReceipt(
                        row.get("actor_id", String.class), row.get("event_id", String.class),
                        row.get("post_id", String.class), row.get("impression_id", String.class),
                        row.get("payload_hash", String.class), row.get("computed_score", Integer.class),
                        row.get("accepted_at", Instant.class))).one();
    }

    public Mono<PostInteractionReceipt> insert(PostInteractionReceipt receipt) {
        return db.sql("""
                INSERT INTO post_interaction_receipts
                (actor_id,event_id,post_id,impression_id,payload_hash,computed_score,accepted_at)
                VALUES (:actor,:event,:post,:impression,:hash,:score,:accepted)
                """).bind("actor", receipt.actorId()).bind("event", receipt.eventId())
                .bind("post", receipt.postId()).bind("impression", receipt.impressionId())
                .bind("hash", receipt.payloadHash()).bind("score", receipt.computedScore())
                .bind("accepted", receipt.acceptedAt()).fetch().rowsUpdated().thenReturn(receipt);
    }

    public Mono<Void> deleteExpired(Instant cutoff, int limit) {
        return db.sql("DELETE FROM post_interaction_receipts WHERE accepted_at < :cutoff ORDER BY accepted_at LIMIT :limit")
                .bind("cutoff", cutoff).bind("limit", limit).fetch().rowsUpdated().then();
    }
}
