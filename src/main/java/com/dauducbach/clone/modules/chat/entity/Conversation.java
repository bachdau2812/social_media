package com.dauducbach.clone.modules.chat.entity;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import lombok.AccessLevel;
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
@FieldDefaults(level = AccessLevel.PRIVATE)
@Table("conversations")
public class Conversation {
    @Id
    String id;
    ConversationType conversationType;
    String title;
    String directKey;
    long lastMessageSeq;
    String lastMessageId;
    Instant lastMessageAt;
    boolean isDissolved;
    String createdBy;
    Instant createdAt;
    Instant updatedAt;
}
