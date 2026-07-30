package com.dauducbach.clone.modules.post.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder
@Table("post_items")
public class PostItem {
    @Id
    String id;
    String postId;
    Integer orderNumber;
    String mediaId;
    String caption;
    String musicId;
    Long musicStart;
    Long musicEnd;
    Instant createdAt;
    Instant updatedAt;
}