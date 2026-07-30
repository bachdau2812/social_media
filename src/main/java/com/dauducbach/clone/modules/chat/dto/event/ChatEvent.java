package com.dauducbach.clone.modules.chat.dto.event;

import com.dauducbach.clone.modules.chat.constant.ChatEventType;
import com.dauducbach.clone.modules.chat.dto.response.ChatCursorResponse;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record ChatEvent(
        ChatEventType type,
        String eventId,
        String conversationId,
        String actorId,
        String entityId,
        String targetUserId,
        String occurredAt,
        List<String> recipientIds,
        ChatMessageResponse message,
        Long deliveredSeq,
        Long readSeq
) {
    public ChatEvent {
        recipientIds = recipientIds == null ? List.of() : List.copyOf(recipientIds);
    }

    public static ChatEvent messageCreated(ChatMessageResponse message, List<String> recipientIds) {
        Instant occurredAt = message.createdAt() == null ? Instant.now() : message.createdAt();
        return new ChatEvent(
                ChatEventType.MESSAGE_CREATED,
                UUID.randomUUID().toString(),
                message.conversationId(),
                message.senderId(),
                message.id(),
                null,
                occurredAt.toString(),
                recipientIds,
                message,
                null,
                null);
    }

    public static ChatEvent cursorUpdated(
            String conversationId,
            String actorId,
            List<String> recipientIds,
            ChatCursorResponse cursor
    ) {
        return new ChatEvent(
                ChatEventType.CURSOR_UPDATED,
                UUID.randomUUID().toString(),
                conversationId,
                actorId,
                null,
                null,
                Instant.now().toString(),
                recipientIds,
                null,
                cursor.deliveredSeq(),
                cursor.readSeq());
    }

    public static ChatEvent memberRequested(
            String conversationId,
            String actorId,
            String requestId,
            String targetUserId,
            List<String> adminIds
    ) {
        return new ChatEvent(
                ChatEventType.MEMBER_REQUESTED,
                UUID.randomUUID().toString(),
                conversationId,
                actorId,
                requestId,
                targetUserId,
                Instant.now().toString(),
                adminIds,
                null,
                null,
                null);
    }

    public static ChatEvent groupCreated(
            String conversationId,
            String actorId,
            List<String> recipientIds
    ) {
        return membershipEvent(
                ChatEventType.GROUP_CREATED,
                conversationId,
                actorId,
                null,
                recipientIds);
    }

    public static ChatEvent memberAdded(
            String conversationId,
            String actorId,
            String targetUserId
    ) {
        return membershipEvent(
                ChatEventType.MEMBER_ADDED,
                conversationId,
                actorId,
                targetUserId,
                List.of(targetUserId));
    }

    public static ChatEvent memberRemoved(
            String conversationId,
            String actorId,
            String targetUserId
    ) {
        return membershipEvent(
                ChatEventType.MEMBER_REMOVED,
                conversationId,
                actorId,
                targetUserId,
                List.of(targetUserId));
    }

    private static ChatEvent membershipEvent(
            ChatEventType type,
            String conversationId,
            String actorId,
            String targetUserId,
            List<String> recipientIds
    ) {
        return new ChatEvent(
                type,
                UUID.randomUUID().toString(),
                conversationId,
                actorId,
                conversationId,
                targetUserId,
                Instant.now().toString(),
                recipientIds,
                null,
                null,
                null);
    }
}