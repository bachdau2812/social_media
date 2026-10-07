package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.*;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.response.*;
import com.dauducbach.clone.modules.chat.entity.*;
import com.dauducbach.clone.modules.chat.repository.*;
import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class SendMessageReactionResponseTest {
    @Test void idempotentSendRestoresCurrentPersonalizedReactionSnapshot() {
        ChatMessageRepository messages = mock(ChatMessageRepository.class);
        ChatReadRepository reads = mock(ChatReadRepository.class);
        ConversationMemberRepository members = mock(ConversationMemberRepository.class);
        ChatAccessService access = mock(ChatAccessService.class);
        MessageReactionService reactions = mock(MessageReactionService.class);
        SendMessageService service = new SendMessageService(messages, reads, mock(ConversationRepository.class),
                members, access, new ChatMessageValidator(new MediaPolicyProperties()), new ChatResponseMapper(),
                mock(TransactionalOperator.class), mock(ChatMessageWriter.class),
                mock(MediaAssets.class), reactions);
        String clientId = "123e4567-e89b-12d3-a456-426614174000";
        ChatMessage message = ChatMessage.builder().id("m").conversationId("c").messageSeq(5)
                .senderId("me").clientMessageId(clientId).messageType(MessageType.TEXT).content("hello").build();
        when(access.requireActiveMember("c", "me")).thenReturn(Mono.just(ConversationMember.builder().build()));
        when(members.findActiveUserIds("c")).thenReturn(Flux.just("me", "other"));
        when(messages.findBySenderIdAndClientMessageId("me", clientId)).thenReturn(Mono.just(message));
        when(reads.findAfterSequence("c", 1, 4, 1)).thenReturn(Flux.just(message));
        when(reactions.getSnapshots("me", "c", List.of("m"))).thenReturn(Mono.just(List.of(
                new ReactionSnapshot("m", 5, 8, ReactionType.HEART, true, 3, List.of(new ReactionCount(ReactionType.HEART, 3))))));
        var request = new SendMessageRequest(clientId, MessageType.TEXT, "hello", null, null, "other", null, null);
        StepVerifier.create(service.sendMessage("me", "c", request)).assertNext(response -> {
            assertThat(response.myReaction()).isEqualTo(ReactionType.HEART);
            assertThat(response.likeCount()).isEqualTo(3);
            assertThat(response.reactionVersion()).isEqualTo(8);
        }).verifyComplete();
    }
}
