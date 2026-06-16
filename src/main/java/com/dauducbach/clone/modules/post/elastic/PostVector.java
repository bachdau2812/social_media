package com.dauducbach.clone.modules.post.elastic;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Document(indexName = "post_vector")
public class PostVector {
    @Id
    @Field(name = "post_id")
    String postId;

    @Field(name = "content_vector")
    List<Double> contentVector;

    @Version
    Long version;
}
