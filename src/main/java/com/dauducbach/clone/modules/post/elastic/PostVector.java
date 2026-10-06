package com.dauducbach.clone.modules.post.elastic;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Document(indexName = "post_vector", createIndex = false)
public class PostVector {
    @Id
    @Field(name = "post_id", type = FieldType.Keyword)
    String postId;

    @Field(name = "content_vector", type = FieldType.Dense_Vector, dims = 768)
    List<Double> contentVector;

    @Field(name = "recommendation_vector", type = FieldType.Dense_Vector, dims = 768)
    List<Double> recommendationVector;
    @Field(name = "author_id", type = FieldType.Keyword) String authorId;
    @Field(name = "model", type = FieldType.Keyword) String model;
    @Field(name = "dimension", type = FieldType.Integer) Integer dimension;
    @Field(name = "schema_version", type = FieldType.Integer) Integer schemaVersion;
    @Field(name = "content_fingerprint", type = FieldType.Keyword) String contentFingerprint;
    @Field(name = "source_revision", type = FieldType.Keyword) String sourceRevision;
    @Field(name = "author_vector_version", type = FieldType.Long) Long authorVectorVersion;
    @Field(name = "embedding_state", type = FieldType.Keyword) String embeddingState;
    @Field(name = "deleted", type = FieldType.Boolean) Boolean deleted;
    SeqNoPrimaryTerm seqNoPrimaryTerm;
}
