package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.*;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.*;
import com.dauducbach.clone.modules.chat.entity.ConversationMember;
import com.dauducbach.clone.modules.chat.repository.*;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class MessageReactionService {
    private final MessageReactionRepository reactions;
    private final ConversationRepository conversations;
    private final ConversationMemberRepository members;
    private final ChatAccessService access;
    private final ChatOutboxRepository outbox;
    private final TransactionalOperator tx;

    public Mono<ReactionState> set(String actorId, String conversationId, String messageId, ReactionType reaction) {
        if (reaction == null) return Mono.error(invalid("reaction is required"));
        return mutate(actorId, conversationId, messageId, reaction);
    }

    public Mono<ReactionState> remove(String actorId, String conversationId, String messageId) {
        return mutate(actorId, conversationId, messageId, null);
    }

    private Mono<ReactionState> mutate(String actorId, String conversationId, String messageId, ReactionType reaction) {
        return tx.transactional(Mono.defer(() -> lockedMessage(actorId, conversationId, messageId, true)
                .flatMap(message -> reactions.findMine(messageId, actorId).map(Optional::of)
                        .defaultIfEmpty(Optional.empty()).flatMap(previous -> {
                            boolean changed = previous.orElse(null) != reaction;
                            Mono<Void> write = !changed ? Mono.empty() : (reaction == null
                                    ? reactions.remove(messageId, actorId)
                                    : reactions.set(messageId, actorId, reaction, Instant.now()))
                                    .then(Mono.defer(() -> reactions.incrementVersion(messageId)));
                            return write.then(Mono.defer(() -> reactions.snapshots(
                                            conversationId, List.of(messageId), actorId, 0)))
                                    .flatMap(snapshots -> {
                                        if (snapshots.size() != 1) return Mono.error(forbidden());
                                        ReactionState state = snapshots.getFirst().state(actorId);
                                        if (!changed) return Mono.just(state);
                                        return reactions.eligibleRecipients(conversationId, message.messageSeq())
                                                .distinct().collectList()
                                                .flatMap(recipients -> outbox.append(ChatEvent.reactionChanged(
                                                        conversationId, state, recipients)).thenReturn(state));
                                    });
                        }))));
    }

    // Same conversation -> membership -> message lock order as membership mutations. Recheck access inside transaction.
    private Mono<MessageReactionRepository.MessageRow> lockedMessage(
            String actorId, String conversationId, String messageId, boolean mutation) {
        if (actorId == null || actorId.isBlank() || conversationId == null || conversationId.isBlank()
                || messageId == null || messageId.isBlank()) return Mono.error(invalid("Chat identifiers are required"));
        return conversations.findByIdForUpdate(conversationId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND)))
                .flatMap(conversation -> mutation && conversation.isDissolved()
                        ? Mono.error(new AppException(ErrorCode.CHAT_CONVERSATION_DISSOLVED))
                        : members.findMembershipForUpdate(conversationId, actorId)
                            .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE)
                            .switchIfEmpty(Mono.error(forbidden())))
                .flatMap(member -> reactions.lockMessage(conversationId, messageId)
                        .filter(message -> message.messageSeq() >= visibleFrom(member)
                                && message.deletedAt() == null && message.messageType() != MessageType.SYSTEM)
                        .switchIfEmpty(Mono.error(forbidden())));
    }

    public Mono<List<ReactionSnapshot>> getSnapshots(String actorId, String conversationId, List<String> messageIds) {
        if (messageIds == null || messageIds.size() > 100
                || messageIds.stream().anyMatch(id -> id == null || id.isBlank()))
            return Mono.error(invalid("Supply at most 100 message IDs"));
        List<String> ids = messageIds.stream().distinct().toList();
        return access.requireActiveMember(conversationId, actorId)
                .flatMap(member -> reactions.snapshots(conversationId, ids, actorId, visibleFrom(member)))
                .flatMap(snapshots -> snapshots.size() == ids.size()
                        ? Mono.just(snapshots) : Mono.error(forbidden()));
    }

    public Mono<CursorPageResponse<MessageReactorResponse>> list(String actorId, String conversationId,
            String messageId, ReactionType filter, String cursor, int limit) {
        if (cursor != null && (cursor.length() > 64 || cursor.isBlank()))
            return Mono.error(invalid("Invalid reactor cursor"));
        int pageSize = limit <= 0 ? 30 : Math.min(limit, 100);
        return tx.transactional(Mono.defer(() -> lockedMessage(actorId, conversationId, messageId, false)
                .thenMany(reactions.list(messageId, filter, cursor == null ? "" : cursor, pageSize + 1))
                .collectList().map(rows -> {
                    boolean hasMore = rows.size() > pageSize;
                    List<MessageReactorResponse> items = List.copyOf(rows.subList(0, Math.min(rows.size(), pageSize)));
                    String next = hasMore ? items.getLast().userId() : null;
                    return new CursorPageResponse<>(items, next, hasMore);
                })));
    }

    private long visibleFrom(ConversationMember member) {
        return ChatVisibility.visibleFromSequence(member.getJoinedSeq(), member.getLastDeletedMessageSeq());
    }

    private AppException forbidden() { return new AppException(ErrorCode.CONVERSATION_FORBIDDEN, "Message is not available for reactions"); }
    private AppException invalid(String message) { return new AppException(ErrorCode.CHAT_REQUEST_INVALID, message); }
}
