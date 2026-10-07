package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.publicapi.ChatNotificationConversation;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ChatNotificationQueryServiceTest {
    private final ConversationRepository conversations = mock(ConversationRepository.class);
    private final ChatNotificationQueryService service = new ChatNotificationQueryService(
            conversations, mock(ConversationMemberRepository.class));

    @Test
    void groupExposesNameWithoutLeakingPersistenceModel() {
        when(conversations.findById("group-1")).thenReturn(Mono.just(Conversation.builder()
                .conversationType(ConversationType.GROUP).title("Nhóm dự án").build()));

        StepVerifier.create(service.findConversation("group-1"))
                .expectNext(new ChatNotificationConversation(true, "Nhóm dự án")).verifyComplete();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "   ")
    void unnamedGroupUsesSafeTitle(String title) {
        when(conversations.findById("group-1")).thenReturn(Mono.just(Conversation.builder()
                .conversationType(ConversationType.GROUP).title(title).build()));

        StepVerifier.create(service.findConversation("group-1"))
                .expectNext(new ChatNotificationConversation(true, "Nhóm chat")).verifyComplete();
    }

    @Test
    void directConversationIsNotMistakenForGroupEvenWithTitle() {
        when(conversations.findById("direct-1")).thenReturn(Mono.just(Conversation.builder()
                .conversationType(ConversationType.DIRECT).title("Alias").build()));

        StepVerifier.create(service.findConversation("direct-1"))
                .expectNext(new ChatNotificationConversation(false, "Alias")).verifyComplete();
    }

    @Test
    void absentConversationDoesNotFabricateGroupDetails() {
        when(conversations.findById("missing")).thenReturn(Mono.empty());

        StepVerifier.create(service.findConversation("missing")).verifyComplete();
    }
}
