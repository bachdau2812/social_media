package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.request.StoryContextRequest;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatMessageRepository;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SendMessageServiceTest {

    @Mock ChatMessageRepository messageRepository;
    @Mock ChatReadRepository chatReadRepository;
    @Mock ConversationRepository conversationRepository;
    @Mock ConversationMemberRepository memberRepository;
    @Mock ChatAccessService accessService;
    @Mock ChatEventPublisher eventPublisher;
    @Mock TransactionalOperator transactionalOperator;
    @Mock R2dbcEntityTemplate entityTemplate;
    @Mock ReactiveInsertOperation.ReactiveInsert<ChatMessage> insertSpec;
    @Mock MediaCompatibilityFacade mediaFacade;
    @Mock MediaService mediaService;
    @Captor ArgumentCaptor<ChatMessage> messageCaptor;

    private SendMessageService service;

    @BeforeEach
    void setUp() {
        service = new SendMessageService(
                messageRepository,
                chatReadRepository,
                conversationRepository,
                memberRepository,
                accessService,
                new ChatMessageValidator(),
                new ChatResponseMapper(),
                eventPublisher,
                transactionalOperator,
                entityTemplate,
                mediaFacade,
                mediaService);
        when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
    }

    @Test
    void persistsStoryContextWithoutInvokingMediaProvider() {
        Instant expiresAt = Instant.parse("2026-08-01T00:00:00Z");
        StoryContextRequest context = new StoryContextRequest(
                "story-1", "owner-1", "VIDEO", 12400L, expiresAt);
        SendMessageRequest request = new SendMessageRequest(
                UUID.randomUUID().toString(), MessageType.STORY_REPLY, "hello", null,
                null, "owner-1", null, context);
        Conversation conversation = Conversation.builder()
                .id("conversation-1")
                .conversationType(ConversationType.DIRECT)
                .lastMessageSeq(4L)
                .build();

        when(accessService.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().build()));
        when(memberRepository.findActiveUserIds("conversation-1"))
                .thenReturn(Flux.just("actor-1", "owner-1"));
        when(messageRepository.findBySenderIdAndClientMessageId("actor-1", request.clientMessageId()))
                .thenReturn(Mono.empty());
        when(conversationRepository.findByIdForUpdate("conversation-1"))
                .thenReturn(Mono.just(conversation));
        when(entityTemplate.insert(ChatMessage.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(ChatMessage.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(conversationRepository.updateMessageSummary(any(), any(Long.class), any(), any()))
                .thenReturn(Mono.just(1));
        when(chatReadRepository.findAfterSequence("conversation-1", 1L, 4L, 1))
                .thenReturn(Flux.empty());
        when(eventPublisher.publish(any())).thenReturn(Mono.empty());

        StepVerifier.create(service.sendMessage("actor-1", "conversation-1", request))
                .assertNext(response -> {
                    assertThat(response.messageType()).isEqualTo(MessageType.STORY_REPLY);
                    assertThat(response.storyContext()).isNotNull();
                    assertThat(response.storyContext().previewAtMs()).isEqualTo(12400L);
                })
                .verifyComplete();

        verify(insertSpec).using(messageCaptor.capture());
        assertThat(messageCaptor.getValue().getMetadata())
                .contains("\"storyId\":\"story-1\"")
                .contains("\"previewAtMs\":12400");
        verify(mediaFacade, never()).fetchMediaByPublicId(any());
        verify(mediaService, never()).registerFetchedMedia(any(), any(), any());
    }
}
