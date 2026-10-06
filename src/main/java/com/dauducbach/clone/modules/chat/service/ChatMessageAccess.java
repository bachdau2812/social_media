package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.ChatMessageActionsRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

/** Call within a transaction. Lock conversations before membership and message rows. */
@Service
@RequiredArgsConstructor
public class ChatMessageAccess {
    private final ConversationRepository conversations;
    private final ConversationMemberRepository members;
    private final ChatMessageActionsRepository messages;

    public Mono<Conversation> lockConversation(String conversationId) {
        if (conversationId == null || conversationId.isBlank()) {
            return Mono.error(invalid("conversationId is required"));
        }
        return conversations.findByIdForUpdate(conversationId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND)));
    }

    public Mono<Context> member(Conversation conversation, String actorId, boolean writable) {
        if (actorId == null || actorId.isBlank()) {
            return Mono.error(invalid("actorId is required"));
        }
        if (writable && conversation.isDissolved()) {
            return Mono.error(new AppException(ErrorCode.CHAT_CONVERSATION_DISSOLVED));
        }
        return members.findMembershipForUpdate(conversation.getId(), actorId)
                .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE)
                .switchIfEmpty(Mono.error(forbidden()))
                .map(member -> new Context(conversation, member));
    }

    public Mono<Context> context(String actorId, String conversationId, boolean writable) {
        return lockConversation(conversationId).flatMap(conversation -> member(conversation, actorId, writable));
    }

    public Mono<ChatMessage> message(Context context, String messageId) {
        if (messageId == null || messageId.isBlank()) {
            return Mono.error(invalid("messageId is required"));
        }
        return messages.lockMessage(context.conversation().getId(), messageId)
                .filter(message -> visible(context, message))
                .switchIfEmpty(Mono.error(forbidden()));
    }

    public boolean visible(Context context, ChatMessage message) {
        return context.conversation().getId().equals(message.getConversationId())
                && message.getMessageSeq() >= context.visibleFrom();
    }

    public static AppException forbidden() {
        return new AppException(ErrorCode.CONVERSATION_FORBIDDEN, "Message is not available");
    }

    public static AppException invalid(String text) {
        return new AppException(ErrorCode.CHAT_REQUEST_INVALID, text);
    }

    public record Context(Conversation conversation, ConversationMember member) {
        public long visibleFrom() {
            return ChatVisibility.visibleFromSequence(member.getJoinedSeq(), member.getLastDeletedMessageSeq());
        }
    }
}
