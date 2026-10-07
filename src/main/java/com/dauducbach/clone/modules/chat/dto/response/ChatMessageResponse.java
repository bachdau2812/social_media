package com.dauducbach.clone.modules.chat.dto.response;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.constant.ReactionType;
import java.util.List;

import java.time.Instant;

public record ChatMessageResponse(
        String id, String conversationId, long messageSeq, String clientMessageId,
        String senderId, String senderDisplayName, String senderAvatarUrl,
        MessageType messageType, String content,
        MediaMetadataResponse metadata, Long replyToSeq, ReplyMessageResponse reply,
        Instant createdAt, Instant editedAt, boolean deleted,
        StoryContextResponse storyContext,
        long likeCount, boolean isReact, ReactionType myReaction,
        List<ReactionCount> reactions, long reactionVersion, boolean forwarded) {
    public ChatMessageResponse {
        reactions = reactions == null ? List.of() : List.copyOf(reactions);
        if (deleted) {
            content = null; metadata = null; replyToSeq = null; reply = null; storyContext = null;
            likeCount = 0; isReact = false; myReaction = null; reactions = List.of();
        }
    }

    public ChatMessageResponse(String id, String conversationId, long messageSeq, String clientMessageId,
            String senderId, String senderDisplayName, String senderAvatarUrl, MessageType messageType,
            String content, MediaMetadataResponse metadata, Long replyToSeq, ReplyMessageResponse reply,
            Instant createdAt, Instant editedAt, boolean deleted, StoryContextResponse storyContext,
            long likeCount, boolean isReact, ReactionType myReaction, List<ReactionCount> reactions, long reactionVersion) {
        this(id, conversationId, messageSeq, clientMessageId, senderId, senderDisplayName, senderAvatarUrl,
                messageType, content, metadata, replyToSeq, reply, createdAt, editedAt, deleted, storyContext,
                likeCount, isReact, myReaction, reactions, reactionVersion, false);
    }

    public ChatMessageResponse(String id, String conversationId, long messageSeq, String clientMessageId,
            String senderId, String senderDisplayName, String senderAvatarUrl, MessageType messageType,
            String content, MediaMetadataResponse metadata, Long replyToSeq, ReplyMessageResponse reply,
            Instant createdAt, Instant editedAt, boolean deleted, StoryContextResponse storyContext) {
        this(id, conversationId, messageSeq, clientMessageId, senderId, senderDisplayName, senderAvatarUrl,
                messageType, content, metadata, replyToSeq, reply, createdAt, editedAt, deleted,
                storyContext, 0, false, null, List.of(), 0);
    }

    public ChatMessageResponse withReactions(ReactionSnapshot snapshot) {
        return new ChatMessageResponse(id, conversationId, messageSeq, clientMessageId, senderId,
                senderDisplayName, senderAvatarUrl, messageType, content, metadata, replyToSeq, reply,
                createdAt, editedAt, deleted, storyContext, snapshot.likeCount(), snapshot.isReact(),
                snapshot.myReaction(), snapshot.reactions(), snapshot.reactionVersion(), forwarded);
    }

    public ChatMessageResponse neutralReactions() {
        return new ChatMessageResponse(id, conversationId, messageSeq, clientMessageId, senderId,
                senderDisplayName, senderAvatarUrl, messageType, content, metadata, replyToSeq, reply,
                createdAt, editedAt, deleted, storyContext, likeCount, false, null, reactions, reactionVersion, forwarded);
    }

    public ChatMessageResponse withReply(ReplyMessageResponse value) {
        return new ChatMessageResponse(id, conversationId, messageSeq, clientMessageId, senderId,
                senderDisplayName, senderAvatarUrl, messageType, content, metadata, replyToSeq, value,
                createdAt, editedAt, deleted, storyContext, likeCount, isReact, myReaction, reactions, reactionVersion, forwarded);
    }

    public ChatMessageResponse withForwarded(boolean value) {
        return new ChatMessageResponse(id, conversationId, messageSeq, clientMessageId, senderId,
                senderDisplayName, senderAvatarUrl, messageType, content, metadata, replyToSeq, reply,
                createdAt, editedAt, deleted, storyContext, likeCount, isReact, myReaction, reactions, reactionVersion, value);
    }

    public ChatMessageResponse withReactionVersion(long version) {
        return new ChatMessageResponse(id, conversationId, messageSeq, clientMessageId, senderId,
                senderDisplayName, senderAvatarUrl, messageType, content, metadata, replyToSeq, reply,
                createdAt, editedAt, deleted, storyContext, likeCount, isReact, myReaction, reactions, version, forwarded);
    }
}
