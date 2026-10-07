package com.dauducbach.clone.modules.personalization.interactions;

import com.dauducbach.clone.commons.vector.*;
import com.dauducbach.clone.modules.personalization.infrastructure.redis.*;
import com.dauducbach.clone.commons.constant.EntityType;
import com.dauducbach.clone.modules.audit.publicapi.AuditActionType;
import com.dauducbach.clone.modules.audit.publicapi.AuditEntry;
import com.dauducbach.clone.modules.audit.publicapi.AuditRecorder;
import com.dauducbach.clone.modules.personalization.model.PreferenceInteractionProcessing;
import com.dauducbach.clone.modules.personalization.infrastructure.persistence.PreferenceInteractionProcessingRepository;
import com.dauducbach.clone.modules.personalization.recovery.PreferenceRecoveryCoordinator;
import com.dauducbach.clone.modules.personalization.publicapi.CanonicalInteractionPosition;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteraction;
import com.dauducbach.clone.modules.personalization.publicapi.PreferenceInteractionProcessor;
import com.dauducbach.clone.modules.user.publicapi.UserExistenceQuery;
import com.dauducbach.clone.commons.serialization.GsonUtils;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import com.google.gson.JsonObject;

@Service
@RequiredArgsConstructor
public class PreferenceInteractionProcessingService implements PreferenceInteractionProcessor {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PreferenceInteractionProcessingService.class);
    private final PreferenceInteractionProcessingRepository ledger;
    private final AuditRecorder audit;
    private final ShortTermPreferenceVectorService vectors;
    private final UserVectorCoordinator coordinator;
    private final PreferenceRecoveryCoordinator recovery;
    private final UserExistenceQuery users;

    /** Persist the canonical feed interaction and advance its derived preference state. */
    @Override
    public Mono<Void> apply(PreferenceInteraction event, CanonicalInteractionPosition position) {
        return Mono.defer(() -> loadOrCreate(event, position))
                .flatMap(row -> audit.recordRequiredInteraction(toAuditEntry(event)).then(auditRecorded(row)))
                // Provider/ranking work never runs under the user lease; this getter reads a built recommendation.
                .flatMap(row -> "AUDIT_RECORDED".equals(row.getStatus())
                        ? users.exists(event.userId()).flatMap(exists -> exists ? vectors.recommendation(event) : Mono.just(List.<Double>of()))
                            .map(post -> new Input(row, List.copyOf(post)))
                        : Mono.just(new Input(row, List.of())))
                .flatMap(input -> coordinator.withUserLock(event.userId(), lease ->
                        recovery.reconcilePending(lease)
                                .then(ledger.findById(event.eventId()))
                                .flatMap(row -> processCurrent(event, row, input.post(), lease))))
                .then();
    }

    private record Input(PreferenceInteractionProcessing row, List<Double> post) {}

    /** Maps the canonical feed event into audit-owned vocabulary at the module boundary. */
    static AuditEntry toAuditEntry(PreferenceInteraction event) {
        AuditActionType action = switch (event.action()) {
            case "LIKE" -> AuditActionType.LIKE_POST;
            case "COMMENT" -> AuditActionType.COMMENT_POST;
            case "REPOST" -> AuditActionType.REPOST_POST;
            default -> throw new IllegalArgumentException("Unsupported canonical post interaction");
        };
        JsonObject metadata = new JsonObject();
        metadata.addProperty("postId", event.postId());
        metadata.addProperty("sourceId", event.sourceId());
        metadata.addProperty("occurredAt", event.occurredAt().toString());
        return new AuditEntry(event.userId(), action, EntityType.POST.name(), event.postId(), "SUCCESS",
                metadata.toString(), event.eventId());
    }

    private Mono<PreferenceInteractionProcessing> loadOrCreate(PreferenceInteraction event, CanonicalInteractionPosition position) {
        return ledger.findById(event.eventId()).switchIfEmpty(Mono.defer(() -> {
            PreferenceInteractionProcessing row = new PreferenceInteractionProcessing();
            row.setSourceEventId(event.eventId()); row.setUserId(event.userId()); row.setPostId(event.postId());
            row.setAction(event.action()); row.setSourceId(event.sourceId()); row.setOccurredAt(micros(event.occurredAt()));
            row.setCanonicalTopic(position.topic()); row.setCanonicalGeneration(position.generation());
            row.setCanonicalPartition(position.partition()); row.setCanonicalOffset(position.offset());
            row.setStatus("RECEIVED"); row.setCreatedAt(micros(Instant.now())); row.setUpdatedAt(row.getCreatedAt());
            return ledger.save(row).onErrorResume(DuplicateKeyException.class, ignored -> ledger.findById(event.eventId()));
        })).map(row -> {
            if (!Objects.equals(row.getUserId(), event.userId()) || !Objects.equals(row.getPostId(), event.postId())
                    || !Objects.equals(row.getAction(), event.action()) || !Objects.equals(row.getSourceId(), event.sourceId())
                    || !Objects.equals(row.getOccurredAt(), micros(event.occurredAt())))
                throw new IllegalStateException("Canonical source identity collision; operator investigation required");
            if (!Objects.equals(row.getCanonicalTopic(), position.topic())
                    || !Objects.equals(row.getCanonicalGeneration(), position.generation())
                    || row.getCanonicalPartition() != position.partition())
                throw continuity("Canonical source moved partition/generation without checkpoint migration");
            return row;
        });
    }

    private Mono<PreferenceInteractionProcessing> auditRecorded(PreferenceInteractionProcessing row) {
        return "RECEIVED".equals(row.getStatus()) ? saveStatus(row, "AUDIT_RECORDED") : Mono.just(row);
    }

    private Mono<Void> processCurrent(PreferenceInteraction event, PreferenceInteractionProcessing row, List<Double> post, VectorLease lease) {
        if (terminal(row)) return Mono.empty();
        if (!"AUDIT_RECORDED".equals(row.getStatus())) return recovery.reconcilePending(lease);
        return users.exists(event.userId()).flatMap(exists -> {
            if (!exists || post.isEmpty()) {
                row.setReason(exists ? "POST_DELETED_REJECTED_OR_NO_VALID_INPUT" : "USER_DELETED");
                row.setOperationId(UUID.randomUUID().toString());
                return coordinator.requireOwner(lease)
                        .then(saveStatus(row, "SKIP_PREPARED"))
                        .flatMap(saved -> recovery.reconcilePending(lease));
            }
            return vectors.updateShortTermVector(event, lease, post).flatMap(desired -> {
                row.setOperationId(UUID.randomUUID().toString());
                row.setDesiredShortVector(GsonUtils.getGson().toJson(desired));
                row.setVectorExpiresAt(Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.MILLIS));
                return coordinator.requireOwner(lease).then(saveStatus(row, "PREPARED"))
                        .flatMap(saved -> recovery.reconcilePending(lease));
            });
        });
    }

    private Mono<PreferenceInteractionProcessing> saveStatus(PreferenceInteractionProcessing row, String status) {
        row.setStatus(status); row.setUpdatedAt(micros(Instant.now()));
        return ledger.save(row);
    }
    private boolean terminal(PreferenceInteractionProcessing row) { return "COMPLETED".equals(row.getStatus()) || "SKIPPED".equals(row.getStatus()); }
    private Instant micros(Instant value) { return value.truncatedTo(ChronoUnit.MICROS); }
    private ShortTermPreferenceRedisState.ContinuityException continuity(String reason) {
        return new ShortTermPreferenceRedisState.ContinuityException(reason + "; quarantine user and repair from durable ledger");
    }
}
