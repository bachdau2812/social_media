package com.dauducbach.clone.modules.chat.entity;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import lombok.AccessLevel;
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
@Builder
@NoArgsConstructor
@AllArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE)
@Table("messages")
public class ChatMessage {
    @Id
    String id;
    String conversationId;
    long messageSeq;
    String clientMessageId;
    String senderId;
    @Transient
    String senderDisplayName;
    @Transient
    String senderAvatarUrl;
    MessageType messageType;
    String content;
    String metadata;
    Long replyToSeq;
    @Transient
    Long replyMessageSeq;
    @Transient
    String replySenderId;
    @Transient
    String replySenderDisplayName;
    @Transient
    MessageType replyMessageType;
    @Transient
    String replyContent;
    @Transient
    String replyMetadata;
    @Transient
    Instant replyDeletedAt;
    Instant createdAt;
    Instant editedAt;
    Instant deletedAt;
}
