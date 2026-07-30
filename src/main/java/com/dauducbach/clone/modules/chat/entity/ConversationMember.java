package com.dauducbach.clone.modules.chat.entity;

import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
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
@Table("conversation_members")
public class ConversationMember {
    @Id
    String id;
    String conversationId;
    String userId;
    String nickname;
    MemberRole memberRole;
    MemberStatus memberStatus;
    long joinedSeq;
    long lastDeliveredSeq;
    long lastReadSeq;
    Long lastDeletedMessageSeq;
    Instant mutedUntil;
    Instant joinedAt;
    Instant leftAt;
}
