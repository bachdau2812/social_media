package com.dauducbach.clone.modules.chat;

import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class ChatPersistenceContractTest {

    @Test
    void exposesOnlyApprovedConversationTypesAndRoles() {
        assertThat(ConversationType.values())
                .containsExactly(ConversationType.DIRECT, ConversationType.GROUP);
        assertThat(MemberRole.values())
                .containsExactly(MemberRole.USER, MemberRole.ADMIN);
    }

    @Test
    void reservesStableChatErrorCodes() {
        assertThat(ErrorCode.CHAT_REQUEST_INVALID.getCode()).isEqualTo(1300);
        assertThat(ErrorCode.CONVERSATION_NOT_FOUND.getCode()).isEqualTo(1301);
        assertThat(ErrorCode.CHAT_MESSAGE_CREATE_FAILED.getCode()).isEqualTo(1323);
        assertThat(ErrorCode.CHAT_EVENT_PUBLISH_FAILED.getCode()).isEqualTo(1343);
    }

    @Test
    void exposesApprovedChatErrorCodeContract() {
        assertThat(ErrorCode.CHAT_REQUEST_INVALID.getHttpStatus()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(ErrorCode.CONVERSATION_FORBIDDEN.getHttpStatus()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(ErrorCode.CHAT_MEMBER_ALREADY_EXISTS.getHttpStatus()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID.getMessage())
                .isEqualTo("Chat message content is invalid");
        assertThat(ErrorCode.CHAT_EVENT_PUBLISH_FAILED.getHttpStatus())
                .isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    @Test
    void conversationUsesUuidAndMonotonicSummaryFields() {
        Conversation conversation = Conversation.builder()
                .id("conversation-1")
                .conversationType(ConversationType.GROUP)
                .lastMessageSeq(0L)
                .build();

        assertThat(conversation.getId()).isEqualTo("conversation-1");
        assertThat(conversation.getLastMessageSeq()).isZero();
    }

    @Test
    void storyReplyMigrationPreservesEveryExistingMessageType() throws IOException {
        String migration = Files.readString(Path.of(
                "src/main/resources/db/manual/story_reply_schema.sql"));

        assertThat(migration)
                .contains("chk_message_type")
                .contains("'TEXT'")
                .contains("'IMAGE'")
                .contains("'VIDEO'")
                .contains("'FILE'")
                .contains("'AUDIO'")
                .contains("'SYSTEM'")
                .contains("'STORY_REPLY'")
                .contains("LIKE_STORY")
                .contains("story_like_dedup_key");
    }
}
