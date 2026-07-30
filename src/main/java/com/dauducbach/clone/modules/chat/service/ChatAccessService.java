package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class ChatAccessService {

    private final ConversationMemberRepository memberRepository;

    public Mono<ConversationMember> requireActiveMember(String conversationId, String userId) {
        return memberRepository.findActive(conversationId, userId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CONVERSATION_FORBIDDEN,
                        "Active chat membership is required")))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(
                                ErrorCode.CONVERSATION_FETCH_FAILED,
                                "Fetch active chat membership failed",
                                error));
    }

    public Mono<ConversationMember> requireAdmin(String conversationId, String userId) {
        return requireActiveMember(conversationId, userId)
                .filter(member -> member.getMemberRole() == MemberRole.ADMIN)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CHAT_ADMIN_REQUIRED,
                        "Active chat admin membership is required")));
    }
}
