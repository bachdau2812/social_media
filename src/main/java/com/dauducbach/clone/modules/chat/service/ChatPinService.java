package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.ConversationType;
import com.dauducbach.clone.modules.chat.constant.MemberRole;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.PinCollectionResponse;
import com.dauducbach.clone.modules.chat.dto.response.PinnedMessageResponse;
import com.dauducbach.clone.modules.chat.publicapi.ChatEvent;
import com.dauducbach.clone.modules.chat.repository.ChatMessageActionsRepository;
import com.dauducbach.clone.modules.chat.repository.ChatOutboxRepository;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class ChatPinService {
    private final ChatMessageAccess access;
    private final ChatMessageActionsRepository actions;
    private final ChatReadRepository reads;
    private final ChatOutboxRepository outbox;
    private final ConversationMemberRepository members;
    private final ChatResponseMapper mapper;
    private final TransactionalOperator tx;
    private final int maxCount;
    private final ChatMessageQueryService hydration;

    public ChatPinService(ChatMessageAccess access, ChatMessageActionsRepository actions, ChatReadRepository reads,
            ChatOutboxRepository outbox, ConversationMemberRepository members, ChatResponseMapper mapper,
            TransactionalOperator tx, int maxCount) {
        this(access, actions, reads, outbox, members, mapper, tx, maxCount, null);
    }

    @Autowired
    public ChatPinService(ChatMessageAccess access, ChatMessageActionsRepository actions, ChatReadRepository reads,
            ChatOutboxRepository outbox, ConversationMemberRepository members, ChatResponseMapper mapper,
            TransactionalOperator tx, @Value("${chat.pins.max-count:5}") int maxCount,
            ChatMessageQueryService hydration) {
        if (maxCount < 1) {
            throw new IllegalArgumentException("chat.pins.max-count must be positive");
        }
        this.access = access;
        this.actions = actions;
        this.reads = reads;
        this.outbox = outbox;
        this.members = members;
        this.mapper = mapper;
        this.tx = tx;
        this.maxCount = maxCount;
        this.hydration = hydration;
    }

    public Mono<PinCollectionResponse> get(String actorId, String conversationId) {
        return tx.transactional(Mono.defer(() -> access.context(actorId, conversationId, false)
                .flatMap(this::collection)));
    }

    public Mono<PinCollectionResponse> put(String actorId, String conversationId, String messageId) {
        return tx.transactional(Mono.defer(() -> access.context(actorId, conversationId, true)
                .flatMap(context -> {
                    if (!canManage(context)) {
                        return Mono.error(new AppException(ErrorCode.CHAT_ADMIN_REQUIRED));
                    }
                    return access.message(context, messageId)
                            .filter(message -> message.getDeletedAt() == null
                                    && message.getMessageType() != MessageType.SYSTEM)
                            .switchIfEmpty(Mono.error(ChatMessageAccess.forbidden()))
                            .then(actions.hasPin(conversationId, messageId))
                            .flatMap(exists -> exists ? collection(context)
                                    : addPin(context, actorId, messageId));
                })));
    }

    private Mono<PinCollectionResponse> addPin(ChatMessageAccess.Context context, String actorId, String messageId) {
        String conversationId = context.conversation().getId();
        return actions.pinCount(conversationId).flatMap(count -> {
            if (count >= maxCount) {
                return Mono.error(ChatMessageAccess.invalid("Pin limit reached"));
            }
            return actions.addPin(conversationId, messageId, actorId, Instant.now())
                    .then(changed(context, actorId));
        });
    }

    public Mono<PinCollectionResponse> remove(String actorId, String conversationId, String messageId) {
        return tx.transactional(Mono.defer(() -> access.context(actorId, conversationId, true)
                .flatMap(context -> {
                    if (!canManage(context)) {
                        return Mono.error(new AppException(ErrorCode.CHAT_ADMIN_REQUIRED));
                    }
                    return access.message(context, messageId)
                            .then(actions.removePin(conversationId, messageId))
                            .flatMap(removed -> removed == 0 ? collection(context) : changed(context, actorId));
                })));
    }

    private Mono<PinCollectionResponse> changed(ChatMessageAccess.Context context, String actorId) {
        String conversationId = context.conversation().getId();
        return actions.incrementPinVersion(conversationId)
                .then(actions.pinVersion(conversationId))
                .flatMap(version -> members.findActiveUserIds(conversationId).distinct().collectList()
                        .flatMap(users -> outbox.append(ChatEvent.pinsChanged(conversationId, actorId, version, users))))
                .then(collection(context));
    }

    private boolean canManage(ChatMessageAccess.Context context) {
        return !context.conversation().isDissolved()
                && (context.conversation().getConversationType() == ConversationType.DIRECT
                        || context.member().getMemberRole() == MemberRole.ADMIN);
    }

    private Mono<PinCollectionResponse> collection(ChatMessageAccess.Context context) {
        String conversationId = context.conversation().getId();
        // Keep the conversation lock until the revision, independent messages and their hydration are complete.
        return actions.pinVersion(conversationId)
                .flatMap(version -> actions.pins(conversationId)
                        .filter(pin -> pin.messageSeq() >= context.visibleFrom())
                        .concatMap(pin -> reads.findAfterSequence(conversationId, context.visibleFrom(),
                                        pin.messageSeq() - 1, 1).next()
                                .filter(message -> message.getId().equals(pin.messageId())
                                        && message.getDeletedAt() == null)
                                .map(message -> new PinnedMessageResponse(mapper.toChatMessageResponse(message),
                                        pin.pinnedBy(), pin.pinnedAt())))
                        .collectList()
                        .flatMap(items -> hydrateCollection(context, version, items)));
    }

    private Mono<PinCollectionResponse> hydrateCollection(ChatMessageAccess.Context context,
            long version, List<PinnedMessageResponse> items) {
        if (hydration == null) {
            return Mono.just(new PinCollectionResponse(version, canManage(context), items));
        }
        return hydration.hydrateMessages(context.member().getUserId(), context.conversation().getId(),
                        items.stream().map(PinnedMessageResponse::message).toList())
                .map(messages -> {
                    var byId = messages.stream().collect(Collectors.toMap(ChatMessageResponse::id, message -> message));
                    List<PinnedMessageResponse> hydrated = items.stream()
                            .map(pin -> new PinnedMessageResponse(byId.get(pin.message().id()),
                                    pin.pinnedBy(), pin.pinnedAt()))
                            .toList();
                    return new PinCollectionResponse(version, canManage(context), hydrated);
                });
    }
}
