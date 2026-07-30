package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ChatAccessServiceTest {

    @Mock
    ConversationMemberRepository memberRepository;

    @Test
    void requireActiveMemberReturnsActiveMember() {
        ChatAccessService service = new ChatAccessService(memberRepository);
        ConversationMember member = member(MemberRole.USER);
        when(memberRepository.findActive("c1", "u1")).thenReturn(Mono.just(member));

        StepVerifier.create(service.requireActiveMember("c1", "u1"))
                .expectNext(member)
                .verifyComplete();
    }

    @Test
    void requireActiveMemberRejectsMissingMembership() {
        ChatAccessService service = new ChatAccessService(memberRepository);
        when(memberRepository.findActive("c1", "u1")).thenReturn(Mono.empty());

        StepVerifier.create(service.requireActiveMember("c1", "u1"))
                .expectErrorSatisfies(error -> assertChatError(error, ErrorCode.CONVERSATION_FORBIDDEN))
                .verify();
    }

    @Test
    void requireAdminRejectsActiveUserRole() {
        ChatAccessService service = new ChatAccessService(memberRepository);
        when(memberRepository.findActive("c1", "u1"))
                .thenReturn(Mono.just(member(MemberRole.USER)));

        StepVerifier.create(service.requireAdmin("c1", "u1"))
                .expectErrorSatisfies(error -> assertChatError(error, ErrorCode.CHAT_ADMIN_REQUIRED))
                .verify();
    }

    @Test
    void requireAdminReturnsActiveAdmin() {
        ChatAccessService service = new ChatAccessService(memberRepository);
        ConversationMember admin = member(MemberRole.ADMIN);
        when(memberRepository.findActive("c1", "u1")).thenReturn(Mono.just(admin));

        StepVerifier.create(service.requireAdmin("c1", "u1"))
                .expectNext(admin)
                .verifyComplete();
    }

    private ConversationMember member(MemberRole role) {
        return ConversationMember.builder()
                .conversationId("c1")
                .userId("u1")
                .memberRole(role)
                .memberStatus(MemberStatus.ACTIVE)
                .build();
    }

    private void assertChatError(Throwable error, ErrorCode expected) {
        assertThat(error).isInstanceOf(AppException.class);
        assertThat(((AppException) error).getErrorCode()).isEqualTo(expected);
    }
}
