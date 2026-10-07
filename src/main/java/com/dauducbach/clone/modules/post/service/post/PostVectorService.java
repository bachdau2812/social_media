package com.dauducbach.clone.modules.post.service.post;
import com.dauducbach.clone.modules.embedding.publicapi.TextEmbeddingProvider;
import com.dauducbach.clone.modules.post.publicapi.PostAuthorPreferenceContext;
import com.dauducbach.clone.modules.post.publicapi.PostAuthorPreferenceQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.time.Duration;
import java.util.Objects;
import com.dauducbach.clone.commons.vector.VectorMath;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import reactor.core.publisher.Flux;
@Service
@RequiredArgsConstructor
public class PostVectorService {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(PostVectorService.class);
    private final TextEmbeddingProvider embeddings;
    private final PostVectorStore store;
    private final PostEmbeddingSourceReader sources;
    private final PostAuthorPreferenceQuery authorPreferences;
    // Bounded per-process raw cache also retains successful provider work across failed ES commits.
    // Failed/empty provider responses expire immediately so retries can recover.
    private final Map<String, Mono<List<Double>>> rawCache = Collections.synchronizedMap(new LinkedHashMap<>(128, .75f, true) {
        @Override protected boolean removeEldestEntry(Map.Entry<String, Mono<List<Double>>> entry) { return size() > 256; }
    });
    public Mono<Void> rebuild(String postId) {
        return Mono.defer(() -> sources.load(postId).map(java.util.Optional::of).defaultIfEmpty(java.util.Optional.empty()).flatMap(maybeSource -> {
            if (maybeSource.isEmpty()) return store.load(postId).defaultIfEmpty(new PostVector())
                    .flatMap(doc -> store.retainDeletionTombstone(postId, doc.getAuthorId()));
            var source = maybeSource.get();
            String status = source.post().getValidateStatus();
            boolean rejected = "REJECTED".equalsIgnoreCase(status);
            if (!rejected && !"APPROVED".equalsIgnoreCase(status)) return Mono.error(new PostVectorPendingException(postId, "moderation pending"));
            // Capture OCC BEFORE snapshot/provider work; never reread a newer baseline at commit.
            return store.load(postId).defaultIfEmpty(new PostVector()).flatMap(baseline -> {
                if (Boolean.TRUE.equals(baseline.getDeleted())) return Mono.empty();
                if (rejected) {
                    PostVector rejectedDocument = PostVector.builder().postId(postId).authorId(source.post().getUserId())
                            .model(VectorMath.MODEL).dimension(VectorMath.DIMENSION).schemaVersion(VectorMath.SCHEMA_VERSION)
                            .contentFingerprint(source.fingerprint()).sourceRevision(source.revision()).authorVectorVersion(0L)
                            .embeddingState("REJECTED").deleted(false).build();
                    return commitIfCurrent(source, baseline, rejectedDocument);
                }
                if (Objects.equals(source.revision(), baseline.getSourceRevision()) && Objects.equals(source.fingerprint(), baseline.getContentFingerprint()) && compatible(baseline)
                        && (source.text().isBlank() ? baseline.getContentVector() == null || baseline.getContentVector().isEmpty()
                            : valid(baseline.getContentVector()))
                        && ("READY".equals(baseline.getEmbeddingState()) && valid(baseline.getRecommendationVector())
                        || "SKIPPED_NO_INPUT".equals(baseline.getEmbeddingState()) && source.text().isBlank()
                            && (baseline.getRecommendationVector() == null || baseline.getRecommendationVector().isEmpty()))) return Mono.empty();
                String authorId = source.post().getUserId();
                Mono<PostAuthorPreferenceContext> author = authorId == null || authorId.isBlank()
                        ? Mono.empty() : authorPreferences.loadPostAuthorContext(authorId);
                // Snapshot service acquires/releases the user lease. Provider runs only after it completes.
                return author.defaultIfEmpty(new PostAuthorPreferenceContext(0, List.of(), List.of(), VectorMath.MODEL))
                        .flatMap(snapshot -> content(source, baseline).flatMap(raw -> {
                            VectorMath.requireCompatible(snapshot.model(), VectorMath.DIMENSION, VectorMath.SCHEMA_VERSION);
                            List<Double> authorVector = !snapshot.longTerm().isEmpty() ? VectorMath.normalize(snapshot.longTerm())
                                    : !snapshot.profile().isEmpty() ? VectorMath.normalize(snapshot.profile()) : List.of();
                            List<Double> recommendation = raw.isEmpty() ? authorVector : authorVector.isEmpty() ? raw
                                    : VectorMath.mix(raw, .7, authorVector, .3);
                            if (raw.isEmpty() || authorVector.isEmpty()) log.debug("|PostVectorService|fallback|postId={}|mode={}", postId,
                                    recommendation.isEmpty() ? "SKIPPED_NO_INPUT" : raw.isEmpty() ? "AUTHOR_ONLY" : "CONTENT_ONLY");
                            PostVector document = PostVector.builder().postId(postId).authorId(authorId)
                                    .contentVector(raw.isEmpty() ? null : raw).recommendationVector(recommendation.isEmpty() ? null : recommendation)
                                    .model(VectorMath.MODEL).dimension(VectorMath.DIMENSION).schemaVersion(VectorMath.SCHEMA_VERSION)
                                    .contentFingerprint(source.fingerprint()).sourceRevision(source.revision()).authorVectorVersion(snapshot.version())
                                    .embeddingState(recommendation.isEmpty() ? "SKIPPED_NO_INPUT" : "READY").deleted(false).build();
                            return commitIfCurrent(source, baseline, document);
                        }));
            });
        }));
    }
    private Mono<Void> commitIfCurrent(PostEmbeddingSourceReader.Source source, PostVector baseline, PostVector document) {
        return sources.load(document.getPostId()).map(java.util.Optional::of).defaultIfEmpty(java.util.Optional.empty()).flatMap(current -> {
            if (current.isEmpty()) return store.retainDeletionTombstone(document.getPostId(), document.getAuthorId());
            if (!Objects.equals(source.revision(), current.get().revision()))
                return Mono.error(new PostVectorPendingException(document.getPostId(), "source changed during embedding"));
            return store.commit(document, baseline);
        });
    }
    private Mono<List<Double>> content(PostEmbeddingSourceReader.Source source, PostVector baseline) {
        if (source.text().isBlank()) return Mono.just(List.of());
        if (Objects.equals(source.fingerprint(), baseline.getContentFingerprint()) && compatible(baseline) && valid(baseline.getContentVector()))
            return Mono.just(VectorMath.normalize(baseline.getContentVector()));
        return rawCache.computeIfAbsent(source.fingerprint(), key -> Mono.defer(() -> embeddings.getEmbedding(source.text()))
                .map(VectorMath::normalize).switchIfEmpty(Mono.error(new IllegalStateException("Embedding provider returned no vector")))
                .cache(vector -> Duration.ofHours(1), error -> Duration.ZERO, () -> Duration.ZERO));
    }
    private boolean compatible(PostVector doc) {
        try { VectorMath.requireCompatible(doc.getModel(), doc.getDimension(), doc.getSchemaVersion()); return true; }
        catch (IllegalArgumentException e) { return false; }
    }
    private boolean valid(List<Double> vector) {
        try { VectorMath.normalize(vector); return true; } catch (IllegalArgumentException e) { return false; }
    }
    public Mono<Void> processPostEmbedding(String postId, String ignoredContent) { return rebuild(postId); }
    /** Trusted lifecycle cleanup: caller captured/authorized the owner before successful SQL deletion. */
    public Mono<Void> deletePost(String postId, String authorId) { return store.retainDeletionTombstone(postId, authorId); }

    /** API retry after SQL source disappeared: check retained author metadata before fencing. */
    public Mono<Void> retryDeletedPost(String postId, String authorId) {
        return sources.load(postId).hasElement().flatMap(exists -> {
            if (exists) return Mono.error(new PostVectorPendingException(postId, "source exists during deletion retry"));
            return store.load(postId).defaultIfEmpty(new PostVector()).flatMap(doc -> {
                    String knownOwner = doc.getAuthorId();
                    if (knownOwner != null && !knownOwner.isBlank() && !knownOwner.equals(authorId))
                        return Mono.error(new IllegalArgumentException("Only the post owner can delete its vector"));
                    if (doc.getPostId() != null && (knownOwner == null || knownOwner.isBlank()))
                        return Mono.error(new PostVectorPendingException(postId, "legacy orphan has no author metadata; ownership repair required"));
                    return store.retainDeletionTombstone(postId, authorId);
                });
        });
    }
    public Mono<Void> deletePostsByAuthor(String authorId, List<String> sourceIds) {
        return Flux.concat(Flux.fromIterable(sourceIds), store.findPostIdsByAuthor(authorId)).distinct()
                .concatMap(id -> store.retainDeletionTombstone(id, authorId)).then();
    }
}
