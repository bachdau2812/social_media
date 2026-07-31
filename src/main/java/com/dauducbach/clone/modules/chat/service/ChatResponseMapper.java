package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationResponse;
import com.dauducbach.clone.modules.chat.dto.response.MediaMetadataResponse;
import com.dauducbach.clone.modules.chat.dto.response.ReplyMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.StoryContextResponse;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.springframework.stereotype.Component;

@Component
public class ChatResponseMapper {

    public ConversationResponse toConversationResponse(ChatReadRepository.ConversationListRow row) {
        long unreadCount = row.unreadCount();
        return new ConversationResponse(
                row.id(),
                row.conversationType(),
                row.dissolved(),
                row.title(),
                row.avatarUrl(),
                row.lastMessageSeq(),
                row.lastMessageId(),
                row.lastMessageAt(),
                row.lastMessageSenderId(),
                row.lastMessageType(),
                lastMessagePreview(row),
                row.currentUserRole(),
                unreadCount,
                row.recipientDeliveredSeq(),
                row.recipientReadSeq(),
                row.createdAt());
    }

    public ConversationResponse toConversationResponse(Conversation conversation, ConversationMember member) {
        return toConversationResponse(conversation, member, conversation.getTitle());
    }

    public ConversationResponse toConversationResponse(
            Conversation conversation,
            ConversationMember member,
            String displayTitle
    ) {
        return new ConversationResponse(
                conversation.getId(),
                conversation.getConversationType(),
                conversation.isDissolved(),
                displayTitle,
                null,
                conversation.getLastMessageSeq(),
                conversation.getLastMessageId(),
                conversation.getLastMessageAt(),
                null,
                null,
                null,
                member.getMemberRole(),
                unreadCount(
                        conversation.getLastMessageSeq(),
                        member.getLastReadSeq(),
                        member.getJoinedSeq(),
                        member.getLastDeletedMessageSeq()),
                0L,
                0L,
                conversation.getCreatedAt());
    }

    public ChatMessageResponse toChatMessageResponse(ChatMessage message) {
        boolean deleted = message.getDeletedAt() != null;
        return new ChatMessageResponse(
                message.getId(),
                message.getConversationId(),
                message.getMessageSeq(),
                message.getClientMessageId(),
                message.getSenderId(),
                message.getSenderDisplayName(),
                message.getSenderAvatarUrl(),
                message.getMessageType(),
                deleted ? null : message.getContent(),
                deleted || !isMediaType(message.getMessageType())
                        ? null : toMediaMetadataResponse(message.getMetadata()),
                message.getReplyToSeq(),
                toReplyMessageResponse(message),
                message.getCreatedAt(),
                message.getEditedAt(),
                deleted,
                deleted || message.getMessageType() != MessageType.STORY_REPLY
                        ? null : toStoryContextResponse(message.getMetadata()));
    }

    private ReplyMessageResponse toReplyMessageResponse(ChatMessage message) {
        if (message.getReplyToSeq() == null) {
            return null;
        }
        if (message.getReplyMessageSeq() == null) {
            return new ReplyMessageResponse(
                    message.getReplyToSeq(), null, null, null, null, null, true);
        }
        boolean deleted = message.getReplyDeletedAt() != null;
        return new ReplyMessageResponse(
                message.getReplyMessageSeq(),
                message.getReplySenderId(),
                message.getReplySenderDisplayName(),
                message.getReplyMessageType(),
                deleted ? null : message.getReplyContent(),
                deleted ? null : toMediaMetadataResponse(message.getReplyMetadata()),
                deleted);
    }
    private String lastMessagePreview(ChatReadRepository.ConversationListRow row) {
        if (row.lastMessageSeq() <= 0 || row.lastMessageType() == null) {
            return null;
        }
        if (row.lastMessageDeletedAt() != null) {
            return "Tin nhắn đã được thu hồi";
        }
        if (row.lastMessageContent() != null && !row.lastMessageContent().isBlank()) {
            return row.lastMessageContent().trim();
        }
        if (row.lastMessageType() == MessageType.STORY_REPLY) {
            return "Đã trả lời một tin";
        }
        return switch (row.lastMessageType()) {
            case IMAGE -> "Đã gửi một ảnh";
            case VIDEO -> "Đã gửi một video";
            case AUDIO -> "Đã gửi một tin nhắn thoại";
            default -> "Đã gửi một tin nhắn";
        };
    }

    private boolean isMediaType(MessageType type) {
        return type == MessageType.IMAGE
                || type == MessageType.VIDEO
                || type == MessageType.FILE
                || type == MessageType.AUDIO;
    }

    public StoryContextResponse toStoryContextResponse(String metadata) {
        if (metadata == null || metadata.isBlank()) {
            return null;
        }
        try {
            JsonObject object = JsonParser.parseString(metadata).getAsJsonObject();
            return new StoryContextResponse(
                    string(object, "storyId"),
                    string(object, "storyOwnerId"),
                    string(object, "mediaType"),
                    longValue(object, "previewAtMs"),
                    instantValue(object, "expiresAt"),
                    null,
                    null);
        } catch (RuntimeException error) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_FETCH_FAILED, "Parse Story reply context failed", error);
        }
    }
    private long unreadCount(long lastMessageSeq, long lastReadSeq, long joinedSeq, Long lastDeletedMessageSeq) {
        long hiddenThrough = Math.max(lastReadSeq, Math.max(joinedSeq - 1,
                lastDeletedMessageSeq == null ? 0L : lastDeletedMessageSeq));
        return Math.max(0, lastMessageSeq - hiddenThrough);
    }

    public MediaMetadataResponse toMediaMetadataResponse(String metadata) {
        if (metadata == null || metadata.isBlank()) {
            return null;
        }
        try {
            JsonObject object = JsonParser.parseString(metadata).getAsJsonObject();
            return new MediaMetadataResponse(
                    string(object, "url"),
                    string(object, "publicId"),
                    string(object, "mimeType"),
                    longValue(object, "size"),
                    string(object, "fileName"),
                    integerValue(object, "width"),
                    integerValue(object, "height"),
                    longValue(object, "duration"));
        } catch (RuntimeException error) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_FETCH_FAILED, "Parse chat message metadata failed", error);
        }
    }

    private String string(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null || value.isJsonNull() ? null : value.getAsString();
    }

    private Long longValue(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null || value.isJsonNull() ? null : value.getAsLong();
    }

    private Integer integerValue(JsonObject object, String name) {
        JsonElement value = object.get(name);
        return value == null || value.isJsonNull() ? null : value.getAsInt();
    }

    private java.time.Instant instantValue(JsonObject object, String name) {
        String value = string(object, name);
        return value == null ? null : java.time.Instant.parse(value);
    }
}
