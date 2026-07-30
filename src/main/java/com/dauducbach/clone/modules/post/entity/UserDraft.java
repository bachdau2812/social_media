package com.dauducbach.clone.modules.post.entity;

import lombok.*;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
@Table("user_drafts")
public class UserDraft {
    @Id
    String id;
    String userId;
    String draftType;
    String thumbnailUrl;
    int mediaCount;
    String captionPreview;
    String payload;
    Instant createdAt;
    Instant updatedAt;
}