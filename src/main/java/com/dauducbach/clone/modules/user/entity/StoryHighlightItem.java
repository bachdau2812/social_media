package com.dauducbach.clone.modules.user.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
@Table("story_highlight_items")
public class StoryHighlightItem {
    @Id
    String id;
    String highlightId;
    String storyId;
    Integer orderNumber;
    Instant createdAt;
}
