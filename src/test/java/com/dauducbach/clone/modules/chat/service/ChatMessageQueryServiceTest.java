package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.response.ChatCursorResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatMessageQueryServiceTest {

    @Test
    void hydratesMultipleStoryRepliesWithOneAvailabilityBatch() {
        ChatAccessService access = mock(ChatAccessService.class);
        ChatReadRepository reads = mock(ChatReadRepository.class);
        ChatCursorService cursors = mock(ChatCursorService.class);
        StoryAvailabilityPort availability = mock(StoryAvailabilityPort.class);
        ChatMessageQueryService service = new ChatMessageQueryService(
                access, reads, new ChatResponseMapper(), cursors, availability);
        Instant expiresAt = Instant.parse("2026-08-01T00:00:00Z");
        ChatMessage first = storyReply(1L, 1000L, expiresAt);
        ChatMessage second = storyReply(2L, 12400L, expiresAt);
        when(access.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().joinedSeq(1L).build()));
        when(reads.findBeforeSequence("conversation-1", 1L, Long.MAX_VALUE, 21))
                .thenReturn(Flux.just(first, second));
        when(cursors.markDelivered("actor-1", "conversation-1", 2L))
                .thenReturn(Mono.just(mock(ChatCursorResponse.class)));
        when(availability.resolve(any(), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var references = (java.util.Collection<StoryAvailabilityPort.StoryReference>) invocation.getArgument(0);
            Map<StoryAvailabilityPort.StoryReference, StoryAvailabilityPort.StoryAvailability> result =
                    new LinkedHashMap<>();
            references.forEach(reference -> result.put(reference, new StoryAvailabilityPort.StoryAvailability(
                    reference.storyId(), true, "VIDEO", reference.previewAtMs(), expiresAt,
                    "https://host/still-" + reference.previewAtMs() + ".jpg")));
            return Mono.just(result);
        });

        StepVerifier.create(service.getMessages("actor-1", "conversation-1", null, null, 20))
                .assertNext(page -> {
                    assertThat(page.items()).hasSize(2);
                    assertThat(page.items().get(0).storyContext().previewUrl())
                            .isEqualTo("https://host/still-1000.jpg");
                    assertThat(page.items().get(1).storyContext().previewUrl())
                            .isEqualTo("https://host/still-12400.jpg");
                })
                .verifyComplete();

        verify(availability, times(1)).resolve(any(), any());
    }

    private ChatMessage storyReply(long sequence, long previewAtMs, Instant expiresAt) {
        return ChatMessage.builder()
                .id("message-" + sequence)
                .conversationId("conversation-1")
                .messageSeq(sequence)
                .clientMessageId("client-" + sequence)
                .senderId("actor-1")
                .messageType(MessageType.STORY_REPLY)
                .content("hello")
                .metadata("""
                        {"storyId":"story-1","storyOwnerId":"owner-1","mediaType":"VIDEO","previewAtMs":%d,"expiresAt":"%s"}
                        """.formatted(previewAtMs, expiresAt))
                .createdAt(Instant.parse("2026-07-31T00:00:00Z").plusSeconds(sequence))
                .build();
    }
}
