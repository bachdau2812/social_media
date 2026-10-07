package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.modules.auth.service.UserAccountQueryService;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ConversationDetailsRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRequestRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.chat.repository.MemberRequestQueryRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConversationMemberLockOrderTest {
    @Mock ChatAccessService accessService;
    @Mock ConversationRepository conversationRepository;
    @Mock ConversationMemberRepository memberRepository;
    @Mock ConversationMemberRequestRepository requestRepository;
    @Mock MemberRequestQueryRepository requestQueryRepository;
    @Mock ConversationDetailsRepository detailsRepository;
    @Mock UserAccountQueryService userAccountQueryService;
    @Mock ChatSystemMessageService systemMessageService;
    @Mock ChatEventPublisher eventPublisher;
    @Mock TransactionalOperator transactionalOperator;
    @Mock R2dbcEntityTemplate entityTemplate;

    @Test
    void leaveLocksConversationBeforeReadingAndUpdatingMembership() {
        Conversation conversation = Conversation.builder()
                .id("conversation-1")
                .conversationType(ConversationType.GROUP)
                .isDissolved(true)
                .lastMessageSeq(8L)
                .build();
        ConversationMember member = ConversationMember.builder()
                .conversationId("conversation-1")
                .userId("actor-1")
                .memberRole(MemberRole.ADMIN)
                .memberStatus(MemberStatus.ACTIVE)
                .build();

        lenient().when(accessService.requireActiveMember("conversation-1", "actor-1"))
                .thenReturn(Mono.just(member));
        when(conversationRepository.findByIdForUpdate("conversation-1")).thenReturn(Mono.just(conversation));
        when(memberRepository.findMembershipForUpdate("conversation-1", "actor-1"))
                .thenReturn(Mono.just(member));
        when(memberRepository.markLeft(any(), any(), any(Long.class), any()))
                .thenReturn(Mono.just(1));
        when(transactionalOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        ConversationMemberService service = new ConversationMemberService(
                accessService,
                conversationRepository,
                memberRepository,
                requestRepository,
                requestQueryRepository,
                detailsRepository,
                userAccountQueryService,
                systemMessageService,
                eventPublisher,
                transactionalOperator,
                entityTemplate);

        StepVerifier.create(service.leaveConversation("actor-1", "conversation-1"))
                .expectNextMatches(result -> result.result().equals("LEFT"))
                .verifyComplete();

        var order = inOrder(conversationRepository, memberRepository);
        order.verify(conversationRepository).findByIdForUpdate("conversation-1");
        order.verify(memberRepository).findMembershipForUpdate("conversation-1", "actor-1");
    }
}
