package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.post.publicapi.StoryQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ChatMessageQueryServiceTest {

    @Test
    void hydratesReactionSnapshotsInOneBatchAfterStoryAvailability() {
        ChatAccessService access = mock(ChatAccessService.class);
        ChatReadRepository reads = mock(ChatReadRepository.class);
        StoryQuery availability = mock(StoryQuery.class);
        MessageReactionService reactions = mock(MessageReactionService.class);
        ChatMessageQueryService service = new ChatMessageQueryService(
                access, reads, new ChatResponseMapper(), availability, reactions);
        Instant expiry = Instant.parse("2026-08-01T00:00:00Z");
        when(access.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().joinedSeq(1L).build()));
        when(reads.findBeforeSequence("conversation-1", 1L, Long.MAX_VALUE, 21))
                .thenReturn(Flux.just(storyReply(1, 1000, expiry), storyReply(2, 2000, expiry)));
        when(availability.resolve(any(), any())).thenReturn(Mono.just(Map.of()));
        when(reactions.getSnapshots("actor-1", "conversation-1", java.util.List.of("message-1", "message-2")))
                .thenReturn(Mono.just(java.util.List.of(new com.dauducbach.clone.modules.chat.dto.response.ReactionSnapshot(
                        "message-1", 1, 9, com.dauducbach.clone.modules.chat.constant.ReactionType.HEART,
                        true, 2, java.util.List.of(new com.dauducbach.clone.modules.chat.dto.response.ReactionCount(
                        com.dauducbach.clone.modules.chat.constant.ReactionType.HEART, 2))))));
        StepVerifier.create(service.getMessages("actor-1", "conversation-1", null, null, 20))
                .assertNext(page -> {
                    var first = page.items().getFirst();
                    assertThat(first.reactionVersion()).isEqualTo(9);
                    assertThat(first.likeCount()).isEqualTo(2);
                    assertThat(first.isReact()).isTrue();
                    assertThat(first.storyContext()).isNotNull();
                    assertThat(first.storyContext().available()).isFalse();
                }).verifyComplete();
        verify(reactions, times(1)).getSnapshots(any(), any(), any());
    }

    @Test
    void hydratesMultipleStoryRepliesWithOneAvailabilityBatch() {
        ChatAccessService access = mock(ChatAccessService.class);
        ChatReadRepository reads = mock(ChatReadRepository.class);
        StoryQuery availability = mock(StoryQuery.class);
        ChatMessageQueryService service = new ChatMessageQueryService(
                access, reads, new ChatResponseMapper(), availability);
        Instant expiresAt = Instant.parse("2026-08-01T00:00:00Z");
        ChatMessage first = storyReply(1L, 1000L, expiresAt);
        ChatMessage second = storyReply(2L, 12400L, expiresAt);
        when(access.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(ConversationMember.builder().joinedSeq(1L).build()));
        when(reads.findBeforeSequence("conversation-1", 1L, Long.MAX_VALUE, 21))
                .thenReturn(Flux.just(first, second));
        when(availability.resolve(any(), any())).thenAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var references = (java.util.Collection<StoryQuery.StoryReference>) invocation.getArgument(0);
            Map<StoryQuery.StoryReference, StoryQuery.StoryAvailability> result =
                    new LinkedHashMap<>();
            references.forEach(reference -> result.put(reference, new StoryQuery.StoryAvailability(
                    reference.storyId(), "owner-1", true, "VIDEO", reference.previewAtMs(), expiresAt,
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

    @Test
    void hydratesSenderAndReplyProfilesWithSingleOwnerBatchesPerPage() {
        ChatAccessService access = mock(ChatAccessService.class);
        ChatReadRepository reads = mock(ChatReadRepository.class);
        StoryQuery stories = mock(StoryQuery.class);
        UserIdentityQuery identities = mock(UserIdentityQuery.class);
        MediaCatalog media = mock(MediaCatalog.class);
        ChatMessage reply = ChatMessage.builder()
                .id("message-2").conversationId("conversation-1").messageSeq(2).senderId("sender-1")
                .messageType(MessageType.TEXT).content("reply").replyToSeq(1L).replyMessageSeq(1L)
                .replySenderId("sender-2").replySenderDisplayName(null).replyMessageType(MessageType.TEXT)
                .replyContent("quoted").build();
        ChatMessage first = ChatMessage.builder()
                .id("message-1").conversationId("conversation-1").messageSeq(1).senderId("sender-1")
                .messageType(MessageType.TEXT).content("hello").build();
        when(access.requireActiveMember("conversation-1", "viewer"))
                .thenReturn(Mono.just(ConversationMember.builder().joinedSeq(1L).build()));
        when(reads.findBeforeSequence("conversation-1", 1L, Long.MAX_VALUE, 3))
                .thenReturn(Flux.just(first, reply));
        when(identities.findIdentities(any())).thenReturn(Flux.just(
                new UserIdentity("sender-1", "one", "Sender One", null),
                new UserIdentity("sender-2", "two", "Sender Two", null)));
        when(media.findCurrentAvatars(any())).thenReturn(Flux.just(new MediaAssetView(
                "avatar-1", "avatar-one", 10, 10, "jpg", "image", 20,
                "https://cdn/avatar.jpg", "https://cdn/avatar-secure.jpg", "sender-1", OwnerType.AVATAR,
                null, null, null, Instant.EPOCH, Instant.EPOCH)));

        ChatMessageQueryService service = new ChatMessageQueryService(
                access, reads, new ChatResponseMapper(), stories, null, identities, media);

        StepVerifier.create(service.getMessages("viewer", "conversation-1", null, null, 2))
                .assertNext(page -> {
                    assertThat(page.items()).hasSize(2);
                    assertThat(page.items().get(0).senderDisplayName()).isEqualTo("Sender One");
                    assertThat(page.items().get(0).senderAvatarUrl()).isEqualTo("https://cdn/avatar-secure.jpg");
                    assertThat(page.items().get(1).reply().senderDisplayName()).isEqualTo("Sender Two");
                }).verifyComplete();

        verify(identities, times(1)).findIdentities(argThat(ids -> ids.size() == 2
                && ids.containsAll(java.util.List.of("sender-1", "sender-2"))));
        verify(media, times(1)).findCurrentAvatars(argThat(ids -> ids.size() == 2
                && ids.containsAll(java.util.List.of("sender-1", "sender-2"))));
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
