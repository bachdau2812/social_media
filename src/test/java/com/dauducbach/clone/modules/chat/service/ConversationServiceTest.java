package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.service.UserAccountQueryService;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.dto.request.CreateDirectConversationRequest;
import com.dauducbach.clone.modules.chat.dto.request.CreateGroupConversationRequest;
import com.dauducbach.clone.modules.chat.dto.response.ConversationResponse;
import com.dauducbach.clone.modules.chat.dto.response.CursorPageResponse;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
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

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConversationServiceTest {

    @Mock ConversationRepository conversationRepository;
    @Mock ConversationMemberRepository memberRepository;
    @Mock UserAccountQueryService userAccountQueryService;
    @Mock ChatReadRepository chatReadRepository;
    @Mock ChatAccessService accessService;
    @Mock ChatResponseMapper mapper;
    @Mock ChatSystemMessageService systemMessageService;
    @Mock ChatEventPublisher eventPublisher;
    @Mock TransactionalOperator transactionalOperator;
    @Mock R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock ReactiveInsertOperation.ReactiveInsert<Conversation> conversationInsertSpec;
    @Mock ReactiveInsertOperation.ReactiveInsert<ConversationMember> memberInsertSpec;
    @Captor ArgumentCaptor<Conversation> conversationCaptor;
    @Captor ArgumentCaptor<ConversationMember> memberCaptor;

    private ConversationService service;

    @BeforeEach
    void setUp() {
        service = new ConversationService(
                conversationRepository, memberRepository, userAccountQueryService,
                chatReadRepository, accessService, mapper, systemMessageService, eventPublisher,
                transactionalOperator, r2dbcEntityTemplate);
        lenient().when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));
        lenient().when(r2dbcEntityTemplate.insert(Conversation.class)).thenReturn(conversationInsertSpec);
        lenient().when(conversationInsertSpec.using(any(Conversation.class))).thenAnswer(invocation -> {
            Conversation conversation = invocation.getArgument(0);
            conversation.setId(conversation.getConversationType() == ConversationType.GROUP ? "g1" : "c1");
            return Mono.just(conversation);
        });
        lenient().when(r2dbcEntityTemplate.insert(ConversationMember.class)).thenReturn(memberInsertSpec);
        lenient().when(memberInsertSpec.using(any(ConversationMember.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));
        lenient().when(eventPublisher.publish(any())).thenReturn(Mono.empty());
        lenient().when(systemMessageService.publish(any())).thenReturn(Mono.empty());
        lenient().when(systemMessageService.insert(any(), any(), any(), any(), any()))
                .thenReturn(Mono.just(new ChatSystemMessageService.SystemMessageResult(null, List.of())));
    }

    @Test
    void createDirectRejectsSelfConversation() {
        StepVerifier.create(service.createDirect("u1", new CreateDirectConversationRequest("u1")))
                .expectErrorSatisfies(error -> assertChatError(error, ErrorCode.DIRECT_CONVERSATION_SELF_NOT_ALLOWED))
                .verify();

        verify(conversationRepository, never()).save(any(Conversation.class));
    }

    @Test
    void createDirectReturnsExistingConversationForSamePair() {
        Conversation existing = conversation("c1", ConversationType.DIRECT, "u1");
        ConversationMember actor = activeMember("c1", "u1", MemberRole.USER);
        ConversationResponse response = response("c1", ConversationType.DIRECT);
        when(userAccountQueryService.exists("u2")).thenReturn(Mono.just(true));
        when(conversationRepository.findByDirectKey(directKey("u1", "u2"))).thenReturn(Mono.just(existing));
        when(memberRepository.findActive("c1", "u1")).thenReturn(Mono.just(actor));
        when(chatReadRepository.findConversation(any(), any())).thenReturn(Mono.just(row("c1", Instant.EPOCH)));
        when(mapper.toConversationResponse(any(ChatReadRepository.ConversationListRow.class))).thenReturn(response);

        StepVerifier.create(service.createDirect("u1", new CreateDirectConversationRequest("u2")))
                .expectNext(response)
                .verifyComplete();

        verify(conversationRepository, never()).save(any(Conversation.class));
        verify(memberRepository, never()).save(any(ConversationMember.class));
    }

    @Test
    void createDirectCreatesTwoActiveUserMembershipsAtomically() {
        Conversation saved = conversation("c1", ConversationType.DIRECT, "u1");
        ConversationMember actor = activeMember("c1", "u1", MemberRole.USER);
        ConversationResponse response = response("c1", ConversationType.DIRECT);
        when(userAccountQueryService.exists("u2")).thenReturn(Mono.just(true));
        when(conversationRepository.findByDirectKey(directKey("u1", "u2"))).thenReturn(Mono.empty());
        when(memberRepository.findActive("c1", "u1")).thenReturn(Mono.just(actor));
        when(chatReadRepository.findConversation(eq("u1"), eq("c1"))).thenReturn(Mono.just(row("c1", Instant.EPOCH)));
        when(mapper.toConversationResponse(any(ChatReadRepository.ConversationListRow.class))).thenReturn(response);

        StepVerifier.create(service.createDirect("u1", new CreateDirectConversationRequest("u2")))
                .expectNext(response)
                .verifyComplete();

        verify(conversationInsertSpec).using(conversationCaptor.capture());
        assertThat(conversationCaptor.getValue().getConversationType()).isEqualTo(ConversationType.DIRECT);
        assertThat(conversationCaptor.getValue().getDirectKey()).isEqualTo(directKey("u1", "u2"));
        verify(memberInsertSpec, times(2)).using(memberCaptor.capture());
        assertThat(memberCaptor.getAllValues())
                .extracting(ConversationMember::getUserId)
                .containsExactlyInAnyOrder("u1", "u2");
        assertThat(memberCaptor.getAllValues())
                .allSatisfy(member -> {
                    assertThat(member.getMemberRole()).isEqualTo(MemberRole.USER);
                    assertThat(member.getMemberStatus()).isEqualTo(MemberStatus.ACTIVE);
                    assertThat(member.getJoinedSeq()).isEqualTo(1L);
                });
    }

    @Test
    void createGroupMakesCreatorAdminAndOtherUsersNormalUsers() {
        Conversation saved = conversation("g1", ConversationType.GROUP, "u1");
        ConversationMember creator = activeMember("g1", "u1", MemberRole.ADMIN);
        ConversationResponse response = response("g1", ConversationType.GROUP);
        when(userAccountQueryService.exists("u2")).thenReturn(Mono.just(true));
        when(userAccountQueryService.exists("u3")).thenReturn(Mono.just(true));
        when(memberRepository.findActive("g1", "u1")).thenReturn(Mono.just(creator));
        when(chatReadRepository.findConversation(eq("u1"), eq("g1"))).thenReturn(Mono.just(row("g1", Instant.EPOCH)));
        when(mapper.toConversationResponse(any(ChatReadRepository.ConversationListRow.class))).thenReturn(response);

        StepVerifier.create(service.createGroup("u1",
                        new CreateGroupConversationRequest("Team", List.of(" u2 ", "u3", "u2", "u1"))))
                .expectNext(response)
                .verifyComplete();

        verify(memberInsertSpec, times(3)).using(memberCaptor.capture());
        assertThat(memberCaptor.getAllValues())
                .filteredOn(member -> member.getUserId().equals("u1"))
                .singleElement()
                .extracting(ConversationMember::getMemberRole)
                .isEqualTo(MemberRole.ADMIN);
        assertThat(memberCaptor.getAllValues())
                .filteredOn(member -> !member.getUserId().equals("u1"))
                .allSatisfy(member -> assertThat(member.getMemberRole()).isEqualTo(MemberRole.USER));
    }

    @Test
    void createGroupRejectsUnknownInitialUser() {
        when(userAccountQueryService.exists("u2")).thenReturn(Mono.just(false));

        StepVerifier.create(service.createGroup("u1", new CreateGroupConversationRequest("Team", List.of("u2"))))
                .expectErrorSatisfies(error -> assertChatError(error, ErrorCode.USER_NOT_FOUND))
                .verify();

        verify(conversationRepository, never()).save(any(Conversation.class));
    }

    @Test
    void getConversationRejectsInactiveMember() {
        when(accessService.requireActiveMember("c1", "u1"))
                .thenReturn(Mono.error(new AppException(ErrorCode.CONVERSATION_FORBIDDEN)));
        lenient().when(conversationRepository.findById("c1")).thenReturn(Mono.empty());

        StepVerifier.create(service.getConversation("u1", "c1"))
                .expectErrorSatisfies(error -> assertChatError(error, ErrorCode.CONVERSATION_FORBIDDEN))
                .verify();
    }

    @Test
    void getConversationsCapsLimitAtOneHundredAndReturnsStableCursor() {
        Instant sortAt = Instant.parse("2026-07-24T00:00:00Z");
        when(chatReadRepository.findConversations(eq("u1"), isNull(), isNull(), eq(101)))
                .thenReturn(Flux.range(0, 101).map(index -> row("c" + index, sortAt.minusSeconds(index))));
        when(mapper.toConversationResponse(any(ChatReadRepository.ConversationListRow.class)))
                .thenAnswer(invocation -> {
                    ChatReadRepository.ConversationListRow argument = invocation.getArgument(0);
                    return response(argument.id(), ConversationType.GROUP);
                });

        StepVerifier.create(service.getConversations("u1", null, 1000))
                .assertNext(page -> {
                    assertThat(page.items()).hasSize(100);
                    assertThat(page.hasMore()).isTrue();
                    assertThat(page.nextCursor()).isNotBlank();
                })
                .verifyComplete();
    }

    @Test
    void getConversationsUsesCursorFromPreviousPage() {
        Instant sortAt = Instant.parse("2026-07-24T00:00:00Z");
        when(chatReadRepository.findConversations(eq("u1"), isNull(), isNull(), eq(2)))
                .thenReturn(Flux.just(row("c1", sortAt), row("c0", sortAt.minusSeconds(1))));
        when(chatReadRepository.findConversations(eq("u1"), eq(sortAt), eq("c1"), eq(2)))
                .thenReturn(Flux.empty());
        when(mapper.toConversationResponse(any(ChatReadRepository.ConversationListRow.class)))
                .thenAnswer(invocation -> {
                    ChatReadRepository.ConversationListRow argument = invocation.getArgument(0);
                    return response(argument.id(), ConversationType.GROUP);
                });

        CursorPageResponse<ConversationResponse> firstPage = service.getConversations("u1", null, 1).block();

        StepVerifier.create(service.getConversations("u1", firstPage.nextCursor(), 1))
                .assertNext(page -> assertThat(page.items()).isEmpty())
                .verifyComplete();
    }

    private Conversation conversation(String id, ConversationType type, String creatorId) {
        return Conversation.builder()
                .id(id)
                .conversationType(type)
                .createdBy(creatorId)
                .createdAt(Instant.parse("2026-07-24T00:00:00Z"))
                .build();
    }

    private ConversationMember activeMember(String conversationId, String userId, MemberRole role) {
        return ConversationMember.builder()
                .id(conversationId + "-" + userId)
                .conversationId(conversationId)
                .userId(userId)
                .memberRole(role)
                .memberStatus(MemberStatus.ACTIVE)
                .joinedSeq(1L)
                .build();
    }

    private ConversationResponse response(String id, ConversationType type) {
        return new ConversationResponse(id, type, false, null, null, 0L, null, null, null, null, null, MemberRole.USER, 0L, 0L, 0L, Instant.EPOCH);
    }

    private ChatReadRepository.ConversationListRow row(String id, Instant sortAt) {
        return new ChatReadRepository.ConversationListRow(
                id, ConversationType.GROUP, false, "Team", null, null, null, null, null,
                0L, null, null, MemberRole.ADMIN, 0L, 1L, null, 0L, 0L, 0L, sortAt, sortAt);
    }

    private String directKey(String firstUserId, String secondUserId) {
        try {
            String source = firstUserId.compareTo(secondUserId) <= 0
                    ? firstUserId + ":" + secondUserId
                    : secondUserId + ":" + firstUserId;
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception error) {
            throw new IllegalStateException(error);
        }
    }

    private void assertChatError(Throwable error, ErrorCode expected) {
        assertThat(error).isInstanceOf(AppException.class);
        assertThat(((AppException) error).getErrorCode()).isEqualTo(expected);
    }
}
