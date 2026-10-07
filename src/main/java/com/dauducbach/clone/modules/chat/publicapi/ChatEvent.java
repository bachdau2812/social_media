package com.dauducbach.clone.modules.chat.publicapi;

import com.dauducbach.clone.modules.chat.dto.response.ChatCursorResponse;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.ReactionState;

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
        Long readSeq,
        ReactionState reactionState,
        Long pinVersion
) {
    public ChatEvent {
        recipientIds = recipientIds == null ? List.of() : List.copyOf(recipientIds);
    }

    public ChatEvent(ChatEventType type, String eventId, String conversationId, String actorId,
            String entityId, String targetUserId, String occurredAt, List<String> recipientIds,
            ChatMessageResponse message, Long deliveredSeq, Long readSeq) {
        this(type, eventId, conversationId, actorId, entityId, targetUserId, occurredAt,
                recipientIds, message, deliveredSeq, readSeq, null);
    }

    public ChatEvent(ChatEventType type, String eventId, String conversationId, String actorId,
            String entityId, String targetUserId, String occurredAt, List<String> recipientIds,
            ChatMessageResponse message, Long deliveredSeq, Long readSeq, ReactionState reactionState) {
        this(type,eventId,conversationId,actorId,entityId,targetUserId,occurredAt,recipientIds,message,deliveredSeq,readSeq,reactionState,null);
    }
    public static ChatEvent messageDeleted(ChatMessageResponse message,String actor,List<String> recipients) {
        return new ChatEvent(ChatEventType.MESSAGE_DELETED,UUID.randomUUID().toString(),message.conversationId(),actor,
            message.id(),null,Instant.now().toString(),recipients,message.neutralReactions(),null,null,null,null);
    }
    public static ChatEvent pinsChanged(String c,String actor,long version,List<String> recipients) {
        return new ChatEvent(ChatEventType.PINS_CHANGED,UUID.randomUUID().toString(),c,actor,null,null,
            Instant.now().toString(),recipients,null,null,null,null,version);
    }

    public static ChatEvent reactionChanged(String conversationId, ReactionState state, List<String> recipients) {
        return new ChatEvent(ChatEventType.MESSAGE_REACTION_CHANGED, UUID.randomUUID().toString(),
                conversationId, state.actorId(), state.messageId(), null, Instant.now().toString(),
                recipients, null, null, null, state);
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
                message.neutralReactions(),
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
