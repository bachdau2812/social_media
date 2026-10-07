package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.vector.*;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class PostVectorRepairService {
    private final PostEmbeddingSourceReader sources;
    private final PostVectorStore store;
    private final PostVectorService vectors;

    public record Inventory(String postId, List<String> findings) {
        public Inventory { findings = List.copyOf(findings); }
    }
    public record Result(String postId, String status, String detail) {}

    public Flux<Inventory> dryRun(List<String> postIds) {
        return Flux.fromIterable(VectorRepairBatch.ids(postIds)).concatMap(this::inspect, 1);
    }

    /** At most 100 explicit IDs, one in flight, 250ms..60s interval. No automatic whole-index backfill. */
    public Flux<Result> apply(List<String> postIds, Duration interval) {
        List<String> ids = VectorRepairBatch.ids(postIds);
        Duration delay = VectorRepairBatch.interval(interval);
        return Flux.fromIterable(ids).concatMap(id -> Mono.delay(delay).then(inspect(id)).flatMap(inventory -> {
                    if (inventory.findings().contains("READY")) return Mono.just(new Result(id, "NO_CHANGE", "Current dual vectors retained"));
                    if (inventory.findings().contains("LIVE_SOURCE_TOMBSTONE"))
                        return Mono.just(new Result(id, "SKIPPED_OPERATOR_RECOVERY_REQUIRED", "Permanent deletion fence retained"));
                    if (inventory.findings().contains("MODERATION_PENDING"))
                        return Mono.just(new Result(id, "SKIPPED_PENDING", "Moderation not final"));
                    // Source recheck, captured OCC, author snapshot and provider placement are shared with lifecycle processing.
                    return vectors.rebuild(id).thenReturn(new Result(id, "REBUILT_OR_FENCED", "Authoritative source and captured OCC; inventory again"));
                }).onErrorResume(VectorRepairRequiredException.class, error -> Mono.just(new Result(id, "QUARANTINED", error.getMessage())))
                .onErrorResume(error -> Mono.just(new Result(id, "FAILED", error.getClass().getSimpleName()))), 1);
    }

    private Mono<Inventory> inspect(String id) {
        return Mono.zip(sources.load(id).map(Optional::of).defaultIfEmpty(Optional.empty()),
                        store.load(id).defaultIfEmpty(new PostVector()))
                .map(state -> {
                    List<String> findings = new ArrayList<>();
                    var source = state.getT1();
                    PostVector document = state.getT2();
                    if (source.isEmpty()) {
                        findings.add("POST_DELETED");
                        if (document.getPostId() != null && (document.getAuthorId() == null || document.getAuthorId().isBlank()))
                            findings.add("LEGACY_ORPHAN_AUTHOR_UNKNOWN");
                        return new Inventory(id, findings);
                    }
                    if (Boolean.TRUE.equals(document.getDeleted())) return new Inventory(id, List.of("LIVE_SOURCE_TOMBSTONE"));
                    String moderation = source.get().post().getValidateStatus();
                    if (!"APPROVED".equalsIgnoreCase(moderation)) return new Inventory(id,
                            List.of("REJECTED".equalsIgnoreCase(moderation) ? "POST_REJECTED" : "MODERATION_PENDING"));
                    if (document.getPostId() == null) findings.add("DOCUMENT_MISSING");
                    try { VectorMath.requireCompatible(document.getModel(), document.getDimension(), document.getSchemaVersion()); }
                    catch (IllegalArgumentException error) { findings.add(document.getModel() == null || document.getDimension() == null
                            || document.getSchemaVersion() == null ? "MODEL_UNKNOWN" : "MODEL_MISMATCH"); }
                    if (!Objects.equals(source.get().revision(), document.getSourceRevision())) findings.add("SOURCE_REVISION_STALE");
                    if (!Objects.equals(source.get().fingerprint(), document.getContentFingerprint())) findings.add("CONTENT_FINGERPRINT_STALE");
                    if (!source.get().text().isBlank()) inspectVector("CONTENT", document.getContentVector(), findings);
                    else if (document.getContentVector() != null && !document.getContentVector().isEmpty()) findings.add("CONTENT_UNEXPECTED_FOR_EMPTY_SOURCE");
                    if ("READY".equals(document.getEmbeddingState())) inspectVector("RECOMMENDATION", document.getRecommendationVector(), findings);
                    else if (!"SKIPPED_NO_INPUT".equals(document.getEmbeddingState()) || !source.get().text().isBlank()) findings.add("RECOMMENDATION_MISSING");
                    if (findings.isEmpty()) findings.add("READY");
                    return new Inventory(id, findings);
                });
    }

    private void inspectVector(String field, List<Double> vector, List<String> findings) {
        if (vector == null || vector.isEmpty()) findings.add(field + "_MISSING");
        else try { VectorMath.normalize(vector); }
        catch (IllegalArgumentException error) { findings.add(field + "_INVALID"); }
    }
}
