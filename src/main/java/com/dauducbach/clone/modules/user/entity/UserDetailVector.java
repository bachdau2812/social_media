package com.dauducbach.clone.modules.user.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

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

    @Field(name = "user_vector")
    List<Double> userVector;

    @Field(name = "user_long_term_vector")
    List<Double> userLongTermVector;
}

