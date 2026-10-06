package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.assertj.core.api.Assertions.assertThat;

class ChatRecallRedactionTest {
    @Test void recalledReplyDoesNotExposeQuoteOrReplySequence() {
        var message = ChatMessage.builder().id("m").messageType(MessageType.TEXT).content("secret")
                .replyToSeq(1L).replyMessageSeq(1L).replyContent("quoted secret")
                .deletedAt(Instant.now()).build();
        var response = new ChatResponseMapper().toChatMessageResponse(message);
        assertThat(response.deleted()).isTrue();
        assertThat(response.content()).isNull();
        assertThat(response.reply()).isNull();
        assertThat(response.replyToSeq()).isNull();
    }
    @Test void visibleReplyToRecalledSourceKeepsOwnBodyButRedactsQuote() {
        var message=ChatMessage.builder().id("reply").messageType(MessageType.TEXT).content("own body")
                .replyToSeq(1L).replyMessageSeq(1L).replyContent("source secret")
                .replyMetadata("{\"url\":\"https://source/secret\"}").replyDeletedAt(Instant.now()).build();
        var response=new ChatResponseMapper().toChatMessageResponse(message);
        assertThat(response.content()).isEqualTo("own body");assertThat(response.reply().deleted()).isTrue();
        assertThat(response.reply().content()).isNull();assertThat(response.reply().metadata()).isNull();
    }
}
