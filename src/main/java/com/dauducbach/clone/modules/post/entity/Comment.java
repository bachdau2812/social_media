package com.dauducbach.clone.modules.post.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Builder

@Table("comments")
public class Comment {
    @Id
    String id;
    String postId;
    String userId;
    String parentId;
    String content;
    String commentType;
    String mediaUrl;
    Instant timestamp;
    @Transient
    long replyCount;
    @Transient
    boolean hasLiked;
}
