package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.modules.chat.constant.*;
import com.dauducbach.clone.modules.chat.entity.*;
import com.dauducbach.clone.modules.chat.dto.request.ForwardMessageRequest;
import com.dauducbach.clone.modules.chat.repository.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import reactor.core.publisher.*;
import reactor.test.StepVerifier;
import java.time.Instant;
import java.util.*;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class ChatMessageActionsTest {
    ConversationRepository conversations = mock(ConversationRepository.class);
    ConversationMemberRepository members = mock(ConversationMemberRepository.class);
    ChatMessageRepository messages = mock(ChatMessageRepository.class);
    ChatMessageActionsRepository actions = mock(ChatMessageActionsRepository.class);
    ChatReadRepository reads = mock(ChatReadRepository.class);
    ChatOutboxRepository outbox = mock(ChatOutboxRepository.class);
    MessageReactionRepository reactions = mock(MessageReactionRepository.class);
    TransactionalOperator tx = mock(TransactionalOperator.class);
    ChatMessageAccess access;
    ChatRecallService recall;
    ChatPinService pins;
    ChatForwardService forward;
    ChatMessageStateService state;
    Conversation conversation;
    ConversationMember member;
    ChatMessage message;
    @BeforeEach void setup() {
        when(tx.transactional(any(Mono.class))).thenAnswer(i -> i.getArgument(0));
        conversation = Conversation.builder().id("c").conversationType(ConversationType.DIRECT).lastMessageSeq(10).build();
        member = ConversationMember.builder().userId("me").memberStatus(MemberStatus.ACTIVE).memberRole(MemberRole.USER).joinedSeq(1).build();
        message = ChatMessage.builder().id("m").conversationId("c").senderId("me").messageSeq(5).messageType(MessageType.TEXT).content("body").createdAt(Instant.EPOCH).build();
        when(conversations.findByIdForUpdate(anyString())).thenAnswer(i -> Mono.just(Conversation.builder().id(i.getArgument(0)).conversationType(ConversationType.DIRECT).lastMessageSeq(10).build()));
        when(conversations.findByIdForUpdate("c")).thenReturn(Mono.just(conversation));
        when(members.findMembershipForUpdate(anyString(), eq("me"))).thenReturn(Mono.just(member));
        when(actions.lockMessage("c", "m")).thenReturn(Mono.just(message));
        when(actions.recall(anyString(), any())).thenReturn(Mono.empty());
        when(actions.removeReactions(anyString())).thenReturn(Mono.empty());
        when(actions.removePin(anyString(), anyString())).thenReturn(Mono.just(0L));
        when(actions.incrementPinVersion(anyString())).thenReturn(Mono.empty());
        when(actions.pinVersion(anyString())).thenReturn(Mono.just(0L));
        when(actions.pins(anyString())).thenReturn(Flux.empty());
        when(actions.pinCount(anyString())).thenReturn(Mono.just(0L));
        when(actions.hasPin(anyString(), anyString())).thenReturn(Mono.just(false));
        when(actions.addPin(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());
        when(reactions.eligibleRecipients(anyString(), anyLong())).thenReturn(Flux.just("me", "peer"));
        when(members.findActiveUserIds(anyString())).thenReturn(Flux.just("me", "peer"));
        when(outbox.append(any())).thenReturn(Mono.empty());
        when(reads.findAfterSequence(eq("c"), anyLong(), eq(4L), eq(1))).thenReturn(Flux.just(message));
        access = new ChatMessageAccess(conversations, members, actions);
        var mapper = new ChatResponseMapper();
        recall = new ChatRecallService(access, actions, outbox, reactions, mapper, tx);
        pins = new ChatPinService(access, actions, reads, outbox, members, mapper, tx, 5);
        state = new ChatMessageStateService(access, messages, reads, mapper, tx);
        var writer = mock(ChatMessageWriter.class);
        when(writer.write(any(), any(), any(), any())).thenAnswer(i -> ((Mono<Void>)i.getArgument(2)).thenReturn(i.getArgument(0)));
        forward = new ChatForwardService(access, actions, messages, writer, members, mapper, tx);
        when(messages.findBySenderIdAndClientMessageId(anyString(), anyString())).thenReturn(Mono.empty());
        when(actions.recordForward(anyString(), anyString(), anyString())).thenReturn(Mono.empty());
        when(actions.referenceMedia(anyString(), anyString())).thenReturn(Mono.just(1L));
    }
    @Test void recallRedactsAndRemovesReactionsWithoutChangingSequence() {
        StepVerifier.create(recall.recall("me", "c", "m")).assertNext(r -> {
            assertThat(r.deleted()).isTrue(); assertThat(r.content()).isNull(); assertThat(r.messageSeq()).isEqualTo(5);
            assertThat(r.reactionVersion()).isEqualTo(1);
        }).verifyComplete();
        verify(actions).removeReactions("m"); verify(outbox).append(any());
        verify(conversations, never()).updateMessageSummary(anyString(), anyLong(), anyString(), any());
    }
    @Test void repeatedRecallIsNoOp() {
        message.setDeletedAt(Instant.now());
        StepVerifier.create(recall.recall("me", "c", "m")).expectNextMatches(r -> r.deleted()).verifyComplete();
        verifyNoInteractions(outbox); verify(actions, never()).recall(any(), any());
    }
    @Test void recallCannotDeletePeerMessage() { message.setSenderId("peer"); fails(recall.recall("me","c","m")); }
    @Test void rejectsInactiveMembership() { member.setMemberStatus(MemberStatus.LEFT); fails(recall.recall("me","c","m")); }
    @Test void rejectsInvisibleHistory() { member.setLastDeletedMessageSeq(5L); fails(recall.recall("me","c","m")); }
    @Test void rejectsBeforeJoin() { member.setJoinedSeq(6); fails(recall.recall("me","c","m")); }
    @Test void dissolvedRecallRejected() { conversation.setDissolved(true); fails(recall.recall("me","c","m")); }
    @Test void recallRemovesPinAndPublishesRevision() {
        when(actions.removePin("c","m")).thenReturn(Mono.just(1L)); when(actions.pinVersion("c")).thenReturn(Mono.just(4L));
        StepVerifier.create(recall.recall("me","c","m")).expectNextCount(1).verifyComplete();
        verify(actions).incrementPinVersion("c"); verify(outbox, times(2)).append(any());
    }
    @Test void outboxFailurePropagatesForRollback() { when(outbox.append(any())).thenReturn(Mono.error(new IllegalStateException("offline"))); StepVerifier.create(recall.recall("me","c","m")).expectError(IllegalStateException.class).verify(); }
    @Test void groupMemberCannotManagePins() { conversation.setConversationType(ConversationType.GROUP); fails(pins.put("me","c","m")); }
    @Test void directMemberCanManagePins() { StepVerifier.create(pins.put("me","c","m")).expectNextMatches(r -> r.canManage()).verifyComplete(); verify(actions).addPin(eq("c"),eq("m"),eq("me"),any()); }
    @Test void groupAdminCanManagePins() { conversation.setConversationType(ConversationType.GROUP); member.setMemberRole(MemberRole.ADMIN); StepVerifier.create(pins.put("me","c","m")).expectNextMatches(r -> r.canManage()).verifyComplete(); }
    @Test void pinLimitRejected() { when(actions.pinCount("c")).thenReturn(Mono.just(5L)); fails(pins.put("me","c","m")); }
    @Test void duplicatePutNoEventEvenAtLimit() { when(actions.hasPin("c","m")).thenReturn(Mono.just(true)); when(actions.pinCount("c")).thenReturn(Mono.just(5L)); StepVerifier.create(pins.put("me","c","m")).expectNextCount(1).verifyComplete(); verifyNoInteractions(outbox); }
    @Test void absentDeleteNoEvent() { StepVerifier.create(pins.remove("me","c","m")).expectNextCount(1).verifyComplete(); verifyNoInteractions(outbox); }
    @Test void dissolvedPinsReadableButCannotManage() { conversation.setDissolved(true); StepVerifier.create(pins.get("me","c")).expectNextMatches(r -> !r.canManage()).verifyComplete(); fails(pins.put("me","c","m")); }
    @Test void stateIncludesTombstones() { message.setDeletedAt(Instant.now()); when(messages.findById("m")).thenReturn(Mono.just(message)); StepVerifier.create(state.get("me","c",List.of("m"))).expectNextMatches(r -> r.getFirst().deleted() && r.getFirst().content()==null).verifyComplete(); }
    @Test void stateRejectsOversizedBatchBeforeAccess() { fails(state.get("me","c",Collections.nCopies(101,"m"))); verifyNoInteractions(conversations); }
    ForwardMessageRequest request() { return new ForwardMessageRequest("c","m","7c5d2f69-292c-4c7e-9630-e5a0fce8c030"); }
    @ParameterizedTest @EnumSource(value=MessageType.class,names={"TEXT","IMAGE","AUDIO"}) void forwardsOwnBodyAndDropsReply(MessageType type) {
        message.setMessageType(type); message.setMetadata(type==MessageType.TEXT?null:"{\"url\":\"https://asset\",\"publicId\":\"asset\"}"); message.setReplyToSeq(1L);
        StepVerifier.create(forward.forward("me","target",request())).assertNext(r -> {
            assertThat(r.forwarded()).isTrue(); assertThat(r.senderId()).isEqualTo("me"); assertThat(r.content()).isEqualTo("body"); assertThat(r.replyToSeq()).isNull(); assertThat(r.messageSeq()).isEqualTo(11);
        }).verifyComplete();
        if(type!=MessageType.TEXT) verify(actions).referenceMedia(anyString(),eq("m"));
    }
    @ParameterizedTest @EnumSource(value=MessageType.class,names={"SYSTEM","STORY_REPLY","VIDEO","FILE"}) void rejectsUnsupportedForward(MessageType type) { message.setMessageType(type); fails(forward.forward("me","target",request())); }
    @Test void sourceDissolvedCanForwardOut() { conversation.setDissolved(true); StepVerifier.create(forward.forward("me","target",request())).expectNextCount(1).verifyComplete(); }
    @Test void recalledSourceCannotForward() { message.setDeletedAt(Instant.now()); fails(forward.forward("me","target",request())); }
    @Test void destinationDissolvedRejected() { when(conversations.findByIdForUpdate("target")).thenReturn(Mono.just(Conversation.builder().id("target").conversationType(ConversationType.DIRECT).isDissolved(true).build())); fails(forward.forward("me","target",request())); }
    @Test void idempotencyMismatchRejected() { when(messages.findBySenderIdAndClientMessageId(anyString(),anyString())).thenReturn(Mono.just(message)); fails(forward.forward("me","target",request())); }
    @Test void retryReturnsAcceptedCopyEvenAfterOriginalRecall() {
        message.setDeletedAt(Instant.now()); var copy=ChatMessage.builder().id("copy").conversationId("target").senderId("me").messageType(MessageType.TEXT).messageSeq(11).forwarded(true).build();
        when(messages.findBySenderIdAndClientMessageId(anyString(),anyString())).thenReturn(Mono.just(copy));
        when(actions.matchesForward("copy","c","m")).thenReturn(Mono.just(true));
        StepVerifier.create(forward.forward("me","target",request())).expectNextMatches(r -> r.id().equals("copy")).verifyComplete(); verifyNoInteractions(outbox);
    }
    @Test void recallCarriesExistingReactionRevisionThroughResponseStateAndEvent() {
        message.setReactionVersion(7);
        StepVerifier.create(recall.recall("me","c","m")).expectNextMatches(r->r.reactionVersion()==8).verifyComplete();
        when(messages.findById("m")).thenReturn(Mono.just(message));
        StepVerifier.create(state.get("me","c",List.of("m"))).expectNextMatches(items->items.getFirst().reactionVersion()==8).verifyComplete();
        var captured=org.mockito.ArgumentCaptor.forClass(com.dauducbach.clone.modules.chat.publicapi.ChatEvent.class);
        verify(outbox).append(captured.capture());assertThat(captured.getValue().message().reactionVersion()).isEqualTo(8);
    }
    @Test void forwardLocksConversationsInSortedOrder() {
        StepVerifier.create(forward.forward("me","target",request())).expectNextCount(1).verifyComplete();
        var order=inOrder(conversations,members,actions);order.verify(conversations).findByIdForUpdate("c");
        order.verify(conversations).findByIdForUpdate("target");order.verify(members).findMembershipForUpdate("c","me");
        order.verify(members).findMembershipForUpdate("target","me");order.verify(actions).lockMessage("c","m");
    }
    @Test void pinCollectionHydratesOldMessagesAndFiltersHistory() {
        when(actions.pinVersion("c")).thenReturn(Mono.just(7L));
        when(actions.pins("c")).thenReturn(Flux.just(new ChatMessageActionsRepository.PinRow("m","peer",Instant.now(),5),
            new ChatMessageActionsRepository.PinRow("hidden","peer",Instant.now(),1)));
        member.setJoinedSeq(2);
        StepVerifier.create(pins.get("me","c")).assertNext(r->{assertThat(r.version()).isEqualTo(7);
            assertThat(r.items()).hasSize(1);assertThat(r.items().getFirst().message().createdAt()).isEqualTo(Instant.EPOCH);
            assertThat(r.items().getFirst().pinnedAt()).isNotEqualTo(Instant.EPOCH);}).verifyComplete();
        verify(reads).findAfterSequence("c",2,4,1);
    }
    @Test void forwardedBadgeSurvivesMappingAndNeutralBroadcast() {
        message.setForwarded(true);var response=new ChatResponseMapper().toChatMessageResponse(message);
        assertThat(response.forwarded()).isTrue();assertThat(com.dauducbach.clone.modules.chat.publicapi.ChatEvent.messageCreated(response,List.of("peer")).message().forwarded()).isTrue();
    }
    @Test void pinEventsExposeRevisionWithoutMessageOrPermission()throws Exception {
        var json=new com.fasterxml.jackson.databind.ObjectMapper().findAndRegisterModules();
        var payload=json.writeValueAsString(com.dauducbach.clone.modules.chat.publicapi.ChatEvent.pinsChanged("c","me",9,List.of("peer")));
        assertThat(payload).contains("\"type\":\"PINS_CHANGED\"","\"pinVersion\":9","\"message\":null").doesNotContain("canManage","sourceMessageId");
    }
    @Test void missingMediaReferenceFailsWithoutCreationEvent() {
        message.setMessageType(MessageType.IMAGE);when(actions.referenceMedia(anyString(),eq("m"))).thenReturn(Mono.just(0L));
        fails(forward.forward("me","target",request()));verifyNoInteractions(outbox);
    }
    @Test void sourceMismatchForRetryRejected() {
        var copy=ChatMessage.builder().id("copy").conversationId("target").senderId("me").messageType(MessageType.TEXT).messageSeq(11).forwarded(true).build();
        when(messages.findBySenderIdAndClientMessageId(anyString(),anyString())).thenReturn(Mono.just(copy));
        when(actions.matchesForward("copy","c","m")).thenReturn(Mono.just(false));fails(forward.forward("me","target",request()));verifyNoInteractions(outbox);
    }
    @Test void acceptedRetryCannotExposeCopyHiddenByDestinationHistoryBoundary() {
        member.setLastDeletedMessageSeq(11L);
        var copy=ChatMessage.builder().id("copy").conversationId("target").senderId("me").messageType(MessageType.TEXT).messageSeq(11).forwarded(true).build();
        when(messages.findBySenderIdAndClientMessageId(anyString(),anyString())).thenReturn(Mono.just(copy));
        when(actions.matchesForward("copy","c","m")).thenReturn(Mono.just(true));
        fails(forward.forward("me","target",request()));
    }
    @Test void acceptedRetryHydratesPersonalReactionsAtItsReportedRevision() {
        var copy=ChatMessage.builder().id("copy").conversationId("target").senderId("me").messageType(MessageType.TEXT)
            .messageSeq(11).reactionVersion(7).forwarded(true).build();
        when(messages.findBySenderIdAndClientMessageId(anyString(),anyString())).thenReturn(Mono.just(copy));
        when(actions.matchesForward("copy","c","m")).thenReturn(Mono.just(true));
        var reactionService=mock(MessageReactionService.class);
        when(reactionService.getSnapshots("me","target",List.of("copy"))).thenReturn(Mono.just(List.of(
            new com.dauducbach.clone.modules.chat.dto.response.ReactionSnapshot("copy",11,7,ReactionType.HEART,true,2,
                List.of(new com.dauducbach.clone.modules.chat.dto.response.ReactionCount(ReactionType.HEART,2))))));
        var mapper=new ChatResponseMapper();
        var hydration=new ChatMessageQueryService(mock(ChatAccessService.class),reads,mapper,mock(com.dauducbach.clone.modules.post.publicapi.StoryQuery.class),reactionService);
        forward=new ChatForwardService(access,actions,messages,mock(ChatMessageWriter.class),members,mapper,tx,hydration);
        StepVerifier.create(forward.forward("me","target",request())).assertNext(r->{
            assertThat(r.reactionVersion()).isEqualTo(7);assertThat(r.likeCount()).isEqualTo(2);
            assertThat(r.myReaction()).isEqualTo(ReactionType.HEART);assertThat(r.isReact()).isTrue();
        }).verifyComplete();verifyNoInteractions(outbox);
    }
    void fails(Mono<?> result) { StepVerifier.create(result).expectError(AppException.class).verify(); }
}
