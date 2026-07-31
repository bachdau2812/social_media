package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.MediaMetadataRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.ConversationResponse;
import com.dauducbach.clone.modules.chat.dto.response.MediaMetadataResponse;
import com.dauducbach.clone.modules.chat.dto.response.ReplyMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.StoryContextResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChatResponseMapperTest {

    private final ChatResponseMapper mapper = new ChatResponseMapper();
    private final ChatMessageValidator validator = new ChatMessageValidator();

    @Test
    void mapsConversationListRowAndCalculatesUnreadCountFromJoinedSequence() {
        Instant createdAt = Instant.parse("2026-07-24T00:00:00Z");
        ChatReadRepository.ConversationListRow row = new ChatReadRepository.ConversationListRow(
                "c1", ConversationType.GROUP, false, "Team", null, "u2", MessageType.TEXT,
                "last", null, 10L, "m10", createdAt, MemberRole.ADMIN,
                6L, 8L, null, 3L, 9L, 8L, createdAt, createdAt);

        ConversationResponse response = mapper.toConversationResponse(row);

        assertThat(response.unreadCount()).isEqualTo(3L);
        assertThat(response.currentUserRole()).isEqualTo(MemberRole.ADMIN);
    }

    @Test
    void mapsAllVisibleMessageFieldsAndValidatorMetadata() {
        Instant createdAt = Instant.parse("2026-07-24T00:00:00Z");
        Instant editedAt = Instant.parse("2026-07-24T00:01:00Z");
        ChatMessageValidator.ValidatedMessage validated = validator.validate(new SendMessageRequest(
                UUID.randomUUID().toString(),
                MessageType.IMAGE,
                null,
                new MediaMetadataRequest("https://host/a.jpg", "p1", "image/jpeg", 10L, "a.jpg", 100, 200, 300L),
                5L,
                null,
                null,
                null));
        ChatMessage message = ChatMessage.builder()
                .id("m1")
                .conversationId("c1")
                .messageSeq(7L)
                .clientMessageId("client-1")
                .senderId("u1")
                .messageType(MessageType.IMAGE)
                .content(null)
                .metadata("{\"url\":\"https://host/a.jpg\",\"publicId\":\"p1\",\"mimeType\":\"image/jpeg\",\"size\":10,\"fileName\":\"a.jpg\",\"width\":100,\"height\":200,\"duration\":300}")
                .replyToSeq(5L)
                .createdAt(createdAt)
                .editedAt(editedAt)
                .build();

        ChatMessageResponse response = mapper.toChatMessageResponse(message);

        assertThat(response).isEqualTo(new ChatMessageResponse(
                "m1", "c1", 7L, "client-1", "u1", null, null, MessageType.IMAGE, null,
                new MediaMetadataResponse("https://host/a.jpg", "p1", "image/jpeg", 10L, "a.jpg", 100, 200, 300L),
                5L, new ReplyMessageResponse(5L, null, null, null, null, null, true), createdAt, editedAt, false, null));
    }

    @Test
    void mapsStoryContextOnlyForStoryReplyMessages() {
        Instant expiresAt = Instant.parse("2026-08-01T00:00:00Z");
        ChatMessage storyReply = ChatMessage.builder()
                .id("m-story")
                .conversationId("c1")
                .messageSeq(8L)
                .clientMessageId("client-story")
                .senderId("u1")
                .messageType(MessageType.STORY_REPLY)
                .content("hello")
                .metadata("""
                        {"storyId":"story-1","storyOwnerId":"owner-1","mediaType":"VIDEO","previewAtMs":12400,"expiresAt":"2026-08-01T00:00:00Z"}
                        """)
                .createdAt(Instant.parse("2026-07-31T00:00:00Z"))
                .build();

        ChatMessageResponse response = mapper.toChatMessageResponse(storyReply);

        assertThat(response.metadata()).isNull();
        assertThat(response.storyContext()).isEqualTo(new StoryContextResponse(
                "story-1", "owner-1", "VIDEO", 12400L, expiresAt, null, null));
    }

    @Test
    void omitsContentAndMetadataForDeletedMessageButKeepsMessageFields() {
        ChatMessage message = ChatMessage.builder()
                .id("m1")
                .conversationId("c1")
                .messageSeq(1L)
                .clientMessageId("client-1")
                .senderId("u1")
                .messageType(MessageType.IMAGE)
                .content("private text")
                .metadata("{\"url\":\"https://host/a.jpg\",\"publicId\":\"p1\",\"mimeType\":\"image/jpeg\",\"size\":10,\"fileName\":\"a.jpg\"}")
                .replyToSeq(2L)
                .createdAt(Instant.parse("2026-07-24T00:00:00Z"))
                .deletedAt(Instant.parse("2026-07-24T00:01:00Z"))
                .build();

        ChatMessageResponse response = mapper.toChatMessageResponse(message);

        assertThat(response.deleted()).isTrue();
        assertThat(response.id()).isEqualTo("m1");
        assertThat(response.messageSeq()).isEqualTo(1L);
        assertThat(response.replyToSeq()).isEqualTo(2L);
        assertThat(response.content()).isNull();
        assertThat(response.metadata()).isNull();
    }

    @Test
    void wrapsMalformedPersistedMetadataAsChatFetchFailure() {
        ChatMessage message = ChatMessage.builder()
                .id("m1")
                .conversationId("c1")
                .messageSeq(1L)
                .messageType(MessageType.IMAGE)
                .metadata("not-json")
                .createdAt(Instant.parse("2026-07-24T00:00:00Z"))
                .build();

        assertThatThrownBy(() -> mapper.toChatMessageResponse(message))
                .isInstanceOfSatisfying(AppException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(ErrorCode.CHAT_MESSAGE_FETCH_FAILED));
    }
}
