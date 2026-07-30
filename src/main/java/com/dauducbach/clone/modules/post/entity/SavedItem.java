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
@Table("saved_items")
public class SavedItem {
    @Id
    String id;
    String userId;
    String postId;
    Instant createdAt;
}
