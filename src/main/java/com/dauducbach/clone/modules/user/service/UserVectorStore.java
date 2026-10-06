package com.dauducbach.clone.modules.user.service;

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch.core.UpdateRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch._types.OpType;
import co.elastic.clients.json.JsonData;
import com.dauducbach.clone.infrastructure.vector.VectorMath;
import lombok.RequiredArgsConstructor;
import org.springframework.data.elasticsearch.client.elc.ReactiveElasticsearchClient;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

/** Atomic partial writes: never save a read document back over another writer's fields. */
@Service
@RequiredArgsConstructor
public class UserVectorStore {
    private static final String INDEX = "user_detail_vector";

    private static final String PROFILE_SCRIPT = """
            if (ctx._source.deleted == true) { throw new IllegalStateException('User vector deleted'); }
            if (ctx._source.user_profile_operation_id == params.operationId) {
                ctx.op = 'noop';
            } else {
                ctx._source.user_vector = params.vector;
                ctx._source.user_vector_model = params.model;
                ctx._source.user_vector_dimension = params.dimension;
                ctx._source.user_vector_schema_version = params.schemaVersion;
                ctx._source.user_profile_operation_id = params.operationId;
                ctx._source.profile_version = (ctx._source.profile_version == null ? 0 : ctx._source.profile_version) + 1;
                if (ctx._source.user_long_term_vector == null || ctx._source.user_long_term_vector.isEmpty()) {
                    ctx._source.user_long_term_vector = params.vector;
                    ctx._source.user_long_term_vector_model = params.model;
                    ctx._source.user_long_term_vector_dimension = params.dimension;
                    ctx._source.user_long_term_vector_schema_version = params.schemaVersion;
                    ctx._source.user_long_term_operation_id = params.operationId;
                    ctx._source.long_term_version = (ctx._source.long_term_version == null ? 0 : ctx._source.long_term_version) + 1;
                    ctx._source.has_learned_history = false;
                }
                ctx._source.vector_version = (ctx._source.vector_version == null ? 0 : ctx._source.vector_version) + 1;
            }
            """;

    private static final String LONG_TERM_SCRIPT = """
            if (ctx._source.deleted == true) { throw new IllegalStateException('User vector deleted'); }
            if (ctx._source.user_long_term_operation_id == params.operationId) {
                ctx.op = 'noop';
            } else {
                ctx._source.user_long_term_vector = params.vector;
                ctx._source.user_long_term_vector_model = params.model;
                ctx._source.user_long_term_vector_dimension = params.dimension;
                ctx._source.user_long_term_vector_schema_version = params.schemaVersion;
                ctx._source.user_long_term_operation_id = params.operationId;
                ctx._source.has_learned_history = true;
                ctx._source.long_term_version = (ctx._source.long_term_version == null ? 0 : ctx._source.long_term_version) + 1;
                ctx._source.vector_version = (ctx._source.vector_version == null ? 0 : ctx._source.vector_version) + 1;
            }
            """;

    private final ReactiveElasticsearchClient client;

    public Mono<Void> upsertProfileAndSeedLongTerm(String userId, List<Double> vector, String operationId) {
        return Mono.defer(() -> {
            validateIdentity(userId, operationId);
            UpdateRequest<Map, Map> request = UpdateRequest.of(builder -> builder
                    .index(INDEX).id(userId).retryOnConflict(3)
                    .script(script -> script.lang("painless").source(PROFILE_SCRIPT).params(parameters(vector, operationId)))
                    .scriptedUpsert(true).upsert(Map.of("user_id", userId)));
            // Profile retries rerun the seed guard on the current document; safe to retry atomically.
            return client.update(request, Map.class).then();
        }).onErrorMap(this::mapConflict);
    }

    /** Fenced journal write: absent documents are create-only; existing documents use captured OCC. */
    public Mono<Void> upsertProfileAndSeedLongTerm(String userId, List<Double> vector, String operationId,
            Long seqNo, Long primaryTerm) {
        return Mono.defer(() -> {
            validateIdentity(userId, operationId);
            Map<String, JsonData> params = parameters(vector, operationId);
            if (seqNo == null && primaryTerm == null) {
                Map<String, Object> source = new java.util.HashMap<>();
                source.put("user_id", userId);
                for (String prefix : List.of("user_vector", "user_long_term_vector")) {
                    source.put(prefix, VectorMath.normalize(vector)); source.put(prefix + "_model", VectorMath.MODEL);
                    source.put(prefix + "_dimension", VectorMath.DIMENSION); source.put(prefix + "_schema_version", VectorMath.SCHEMA_VERSION);
                }
                source.put("user_profile_operation_id", operationId); source.put("user_long_term_operation_id", operationId);
                source.put("profile_version", 1L); source.put("long_term_version", 1L); source.put("vector_version", 1L);
                source.put("has_learned_history", false);
                return client.index(IndexRequest.of(builder -> builder.index(INDEX).id(userId).opType(OpType.Create).document(source))).then();
            }
            if (seqNo == null || primaryTerm == null || seqNo < 0 || primaryTerm < 1)
                return Mono.error(new IllegalArgumentException("Profile writes require both valid OCC tokens or neither"));
            UpdateRequest<Map, Map> request = UpdateRequest.of(builder -> builder.index(INDEX).id(userId)
                    .ifSeqNo(seqNo).ifPrimaryTerm(primaryTerm)
                    .script(script -> script.lang("painless").source(PROFILE_SCRIPT).params(params)));
            return client.update(request, Map.class).then();
        }).onErrorMap(this::mapConflict);
    }

    /** Commit only a batch containing valid learned interactions. Seed uses the profile method. */
    public Mono<Void> updateLongTerm(String userId, List<Double> vector, String operationId, long seqNo, long primaryTerm) {
        return Mono.defer(() -> {
            validateIdentity(userId, operationId);
            if (seqNo < 0 || primaryTerm < 1) {
                throw new IllegalArgumentException("Long-term updates require valid sequence number and primary term");
            }
            UpdateRequest<Map, Map> request = UpdateRequest.of(builder -> builder
                    .index(INDEX).id(userId).ifSeqNo(seqNo).ifPrimaryTerm(primaryTerm)
                    .script(script -> script.lang("painless").source(LONG_TERM_SCRIPT).params(parameters(vector, operationId))));
            // Never retry the same computed vector against a newer document. Caller must reread/recompute.
            return client.update(request, Map.class).then();
        }).onErrorMap(this::mapConflict);
    }

    /** Permanent deletion fence: replacing the source also removes every vector and its metadata. */
    public Mono<Void> retainDeletionTombstone(String userId) {
        return Mono.defer(() -> {
            if (userId == null || userId.isBlank()) return Mono.error(new IllegalArgumentException("User ID must not be blank"));
            return client.index(IndexRequest.of(builder -> builder.index(INDEX).id(userId).opType(OpType.Index)
                    .document(Map.of("user_id", userId, "deleted", true)))).then();
        });
    }

    private Map<String, JsonData> parameters(List<Double> vector, String operationId) {
        return Map.of("vector", JsonData.of(VectorMath.normalize(vector)),
                "operationId", JsonData.of(operationId), "model", JsonData.of(VectorMath.MODEL),
                "dimension", JsonData.of(VectorMath.DIMENSION), "schemaVersion", JsonData.of(VectorMath.SCHEMA_VERSION));
    }

    private void validateIdentity(String userId, String operationId) {
        if (userId == null || userId.isBlank() || operationId == null || operationId.isBlank()) {
            throw new IllegalArgumentException("User ID and operation ID must not be blank");
        }
    }

    private Throwable mapConflict(Throwable error) {
        if (error instanceof ElasticsearchException elasticsearchError && elasticsearchError.status() == 409) {
            return new RetryableVectorConflictException(elasticsearchError);
        }
        return error;
    }

    public static class RetryableVectorConflictException extends RuntimeException {
        public RetryableVectorConflictException(Throwable cause) {
            super("Vector changed; reread the document and recompute before retrying", cause);
        }
    }
}
