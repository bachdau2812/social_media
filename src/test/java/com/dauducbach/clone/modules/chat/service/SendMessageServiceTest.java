package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.MediaMetadataRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.request.StoryContextRequest;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.repository.ChatMessageRepository;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssets;
import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SendMessageServiceTest {

    @Mock ChatMessageRepository messageRepository;
    @Mock ChatReadRepository chatReadRepository;
    @Mock ConversationRepository conversationRepository;
    @Mock ConversationMemberRepository memberRepository;
    @Mock ChatAccessService accessService;
    @Mock TransactionalOperator transactionalOperator;
    @Mock ChatMessageWriter messageWriter;
    @Mock MediaAssets mediaAssets;
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
                new ChatMessageValidator(new MediaPolicyProperties()),
                new ChatResponseMapper(),
                transactionalOperator,
                messageWriter,
                mediaAssets);
        lenient().when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(messageWriter.write(any(), any(), any(), any()))
                .thenAnswer(invocation -> ((Mono<Void>) invocation.getArgument(2))
                        .thenReturn(invocation.getArgument(0)));
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
        when(chatReadRepository.findAfterSequence("conversation-1", 1L, 4L, 1))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.sendMessage("actor-1", "conversation-1", request))
                .assertNext(response -> {
                    assertThat(response.messageType()).isEqualTo(MessageType.STORY_REPLY);
                    assertThat(response.storyContext()).isNotNull();
                    assertThat(response.storyContext().previewAtMs()).isEqualTo(12400L);
                })
                .verifyComplete();

        verify(messageWriter).write(messageCaptor.capture(), any(), any(), any());
        assertThat(messageCaptor.getValue().getMetadata())
                .contains("\"storyId\":\"story-1\"")
                .contains("\"previewAtMs\":12400");
        assertThat(messageCaptor.getValue().getClientPayloadHash()).hasSize(64);
        verify(mediaAssets, never()).fetchRemoteAsset(any());
        verify(mediaAssets, never()).registerFetchedAsset(any(), any(), any());

        var lockOrder = inOrder(conversationRepository, memberRepository);
        lockOrder.verify(conversationRepository).findByIdForUpdate("conversation-1");
        lockOrder.verify(memberRepository).findActiveUserIds("conversation-1");
    }

    @Test
    void createsAudioMessageWhenCloudinaryAndRequestDimensionsAreUnavailable() {
        MediaMetadataRequest metadata = new MediaMetadataRequest(
                "https://cdn.test/voice.webm",
                "voice-1",
                "audio/webm",
                5L,
                "voice.webm",
                null,
                null,
                1200L);
        SendMessageRequest request = new SendMessageRequest(
                UUID.randomUUID().toString(),
                MessageType.AUDIO,
                null,
                metadata,
                null,
                "recipient-1",
                null,
                null);
        Conversation conversation = Conversation.builder()
                .id("conversation-1")
                .conversationType(ConversationType.DIRECT)
                .lastMessageSeq(4L)
                .build();
        MediaAssetView fetched = new MediaAssetView(
                "asset-1", "voice-1", 0, 0, null, "video", 5, null,
                "https://cdn.test/voice.webm", null, null, null, null, null, null, null);

        when(accessService.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().build()));
        when(memberRepository.findActiveUserIds("conversation-1"))
                .thenReturn(Flux.just("actor-1", "recipient-1"));
        when(messageRepository.findBySenderIdAndClientMessageId("actor-1", request.clientMessageId()))
                .thenReturn(Mono.empty());
        when(mediaAssets.fetchRemoteAsset("voice-1")).thenReturn(Mono.just(fetched));
        when(conversationRepository.findByIdForUpdate("conversation-1"))
                .thenReturn(Mono.just(conversation));
        when(mediaAssets.registerFetchedAsset(any(MediaAssetView.class), any(), eq(OwnerType.CHAT_MESSAGE)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        when(chatReadRepository.findAfterSequence("conversation-1", 1L, 4L, 1))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.sendMessage("actor-1", "conversation-1", request))
                .assertNext(response -> {
                    assertThat(response.messageType()).isEqualTo(MessageType.AUDIO);
                    assertThat(response.metadata()).isNotNull();
                    assertThat(response.metadata().width()).isNull();
                    assertThat(response.metadata().height()).isNull();
                    assertThat(response.metadata().duration()).isEqualTo(1200L);
                })
                .verifyComplete();
    }

    @Test
    void rejectsReusingClientMessageIdForDifferentPayload() {
        String clientMessageId = "123e4567-e89b-12d3-a456-426614174000";
        SendMessageRequest retry = new SendMessageRequest(
                clientMessageId, MessageType.TEXT, "different text", null, null, "recipient-1", null, null);
        ChatMessage accepted = ChatMessage.builder()
                .id("message-1")
                .conversationId("conversation-1")
                .messageSeq(5L)
                .clientMessageId(clientMessageId)
                .senderId("actor-1")
                .messageType(MessageType.TEXT)
                .content("original text")
                .createdAt(Instant.parse("2026-08-01T00:00:00Z"))
                .build();

        when(accessService.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().build()));
        when(messageRepository.findBySenderIdAndClientMessageId("actor-1", clientMessageId))
                .thenReturn(Mono.just(accepted));
        StepVerifier.create(service.sendMessage("actor-1", "conversation-1", retry))
                .expectErrorMatches(error -> error instanceof AppException appException
                        && appException.getErrorCode() == ErrorCode.CHAT_MESSAGE_IDEMPOTENCY_CONFLICT)
                .verify();
    }

    @Test
    void replaysLegacyClientMessageIdWhenStoredPayloadMatches() {
        String clientMessageId = "123e4567-e89b-12d3-a456-426614174000";
        SendMessageRequest retry = new SendMessageRequest(
                clientMessageId, MessageType.TEXT, "original text", null, null, "recipient-1", null, null);
        ChatMessage accepted = ChatMessage.builder()
                .id("message-1")
                .conversationId("conversation-1")
                .messageSeq(5L)
                .clientMessageId(clientMessageId)
                .senderId("actor-1")
                .messageType(MessageType.TEXT)
                .content("original text")
                .createdAt(Instant.parse("2026-08-01T00:00:00Z"))
                .build();

        when(accessService.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().build()));
        when(messageRepository.findBySenderIdAndClientMessageId("actor-1", clientMessageId))
                .thenReturn(Mono.just(accepted));
        when(chatReadRepository.findAfterSequence("conversation-1", 1L, 4L, 1))
                .thenReturn(Flux.empty());

        StepVerifier.create(service.sendMessage("actor-1", "conversation-1", retry))
                .assertNext(response -> assertThat(response.id()).isEqualTo("message-1"))
                .verifyComplete();
    }
}
