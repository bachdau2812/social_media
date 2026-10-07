package com.dauducbach.clone.modules.personalization.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Document(indexName = "user_detail_vector")
public class UserDetailVector {
    @Id
    @Field(name = "user_id")
    String userId;

    @Field(name = "deleted", type = org.springframework.data.elasticsearch.annotations.FieldType.Boolean)
    Boolean deleted;

    @Field(name = "user_vector")
    List<Double> userVector;

    @Field(name = "user_long_term_vector")
    List<Double> userLongTermVector;

    @Field(name = "user_vector_model")
    String userVectorModel;

    @Field(name = "user_vector_dimension")
    Integer userVectorDimension;

    @Field(name = "user_vector_schema_version")
    Integer userVectorSchemaVersion;

    @Field(name = "user_long_term_vector_model")
    String userLongTermVectorModel;

    @Field(name = "user_long_term_vector_dimension")
    Integer userLongTermVectorDimension;

    @Field(name = "user_long_term_vector_schema_version")
    Integer userLongTermVectorSchemaVersion;

    // Nullable for legacy documents; absence is not proof that no learned history exists.
    @Field(name = "has_learned_history")
    Boolean hasLearnedHistory;

    @Field(name = "profile_version")
    Long profileVersion;

    @Field(name = "long_term_version")
    Long longTermVersion;

    @Field(name = "vector_version")
    Long vectorVersion;

    @Field(name = "user_profile_operation_id")
    String userProfileOperationId;

    @Field(name = "user_long_term_operation_id")
    String userLongTermOperationId;

    // Spring Data treats this type as ES response metadata, not a persisted source field.
    SeqNoPrimaryTerm seqNoPrimaryTerm;
}

