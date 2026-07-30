package com.dauducbach.clone.modules.chat.entity;

import com.dauducbach.clone.modules.chat.constant.MemberRequestStatus;
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
@Table("conversation_member_requests")
public class ConversationMemberRequest {
    @Id
    String id;
    String conversationId;
    String targetUserId;
    String requestedBy;
    MemberRequestStatus requestStatus;
    String resolvedBy;
    Instant resolvedAt;
    String pendingKey;
    Instant createdAt;
}
