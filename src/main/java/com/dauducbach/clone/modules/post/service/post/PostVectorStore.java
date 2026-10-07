package com.dauducbach.clone.modules.post.service.post;
import com.dauducbach.clone.modules.post.elastic.PostVector;
import com.dauducbach.clone.commons.vector.VectorMath;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.json.JsonData;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.*;
@Service
@RequiredArgsConstructor
public class PostVectorStore {
    private static final String INDEX = "post_vector";
    private static final int AUTHOR_PAGE_SIZE = 500;
    private static final String COMMIT_SCRIPT = """
            if (ctx._source.deleted == true) { throw new IllegalStateException('Post vector deleted'); }
            ctx._source = params.document;
            """;
    private final ReactiveElasticsearchClient client;
    private final ReactiveElasticsearchOperations operations;
    public Mono<PostVector> load(String postId) { return Mono.defer(() -> operations.get(postId, PostVector.class)); }
    public Mono<Void> commit(PostVector document, PostVector capturedBaseline) {
        return Mono.defer(() -> {
            Map<String,Object> source = source(document);
            if (Boolean.TRUE.equals(capturedBaseline.getDeleted()))
                return Mono.error(new PostVectorPendingException(document.getPostId(), "deletion fence"));
            if (capturedBaseline.getPostId() == null) {
                return client.index(IndexRequest.of(b -> b.index(INDEX).id(document.getPostId()).opType(OpType.Create).document(source))).then();
            }
            var occ = capturedBaseline.getSeqNoPrimaryTerm();
            if (occ == null || occ.sequenceNumber() < 0 || occ.primaryTerm() < 1)
                return Mono.error(new IllegalArgumentException("Existing post vector requires captured OCC tokens"));
            UpdateRequest<Map,Map> request = UpdateRequest.of(b -> b.index(INDEX).id(document.getPostId())
                    .ifSeqNo(occ.sequenceNumber()).ifPrimaryTerm(occ.primaryTerm())
                    .script(s -> s.lang("painless").source(COMMIT_SCRIPT).params("document", JsonData.of(source))));
            return client.update(request, Map.class).then();
        });
    }
    public Mono<Void> retainDeletionTombstone(String postId, String authorId) {
        return Mono.defer(() -> {
            requireId(postId);
            Map<String,Object> tombstone = new HashMap<>(Map.of("post_id", postId, "deleted", true));
            if (authorId != null && !authorId.isBlank()) tombstone.put("author_id", authorId);
            // Replacement permanently removes every searchable vector, while retaining the ID fence.
            return client.index(IndexRequest.of(b -> b.index(INDEX).id(postId).opType(OpType.Index).document(tombstone))).then();
        });
    }
    public Flux<String> findPostIdsByAuthor(String authorId) {
        return Flux.defer(() -> {
            requireId(authorId);
            return authorPage(authorId, List.of()).expand(page -> page.hits().hits().size() < AUTHOR_PAGE_SIZE
                            ? Mono.empty() : authorPage(authorId, page.hits().hits().getLast().sort()))
                    .concatMapIterable(page -> page.hits().hits()).map(Hit::id);
        });
    }
    private Mono<co.elastic.clients.elasticsearch.core.search.ResponseBody<Map>> authorPage(String authorId, List<FieldValue> after) {
        return client.search(SearchRequest.of(b -> b.index(INDEX).size(AUTHOR_PAGE_SIZE)
                .query(q -> q.term(t -> t.field("author_id").value(authorId)))
                .sort(s -> s.field(f -> f.field("post_id").order(SortOrder.Asc))).searchAfter(after)), Map.class);
    }
    private Map<String,Object> source(PostVector doc) {
        requireId(doc.getPostId()); requireId(doc.getAuthorId());
        VectorMath.requireCompatible(doc.getModel(), doc.getDimension(), doc.getSchemaVersion());
        if (doc.getSourceRevision() == null || doc.getContentFingerprint() == null)
            throw new IllegalArgumentException("Post vector requires source revision and fingerprint");
        if (!Set.of("READY", "SKIPPED_NO_INPUT", "REJECTED").contains(doc.getEmbeddingState()))
            throw new IllegalArgumentException("Post vector requires explicit final embedding state");
        Map<String,Object> result = new HashMap<>();
        result.put("post_id", doc.getPostId()); result.put("author_id", doc.getAuthorId()); result.put("deleted", false);
        result.put("model", doc.getModel()); result.put("dimension", doc.getDimension()); result.put("schema_version", doc.getSchemaVersion());
        result.put("source_revision", doc.getSourceRevision()); result.put("content_fingerprint", doc.getContentFingerprint());
        result.put("embedding_state", doc.getEmbeddingState()); result.put("author_vector_version", doc.getAuthorVectorVersion());
        if (doc.getContentVector() != null && !doc.getContentVector().isEmpty()) result.put("content_vector", VectorMath.normalize(doc.getContentVector()));
        if ("READY".equals(doc.getEmbeddingState())) result.put("recommendation_vector", VectorMath.normalize(doc.getRecommendationVector()));
        else if ((doc.getContentVector() != null && !doc.getContentVector().isEmpty())
                || (doc.getRecommendationVector() != null && !doc.getRecommendationVector().isEmpty()))
            throw new IllegalArgumentException("Skipped post cannot carry vectors");
        return result;
    }
    private void requireId(String value) { if (value == null || value.isBlank()) throw new IllegalArgumentException("Vector identity must not be blank"); }
}
