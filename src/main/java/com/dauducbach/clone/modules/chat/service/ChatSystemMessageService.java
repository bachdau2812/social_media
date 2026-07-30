package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.constant.SystemMessageAction;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.user.service.UserIdentityQueryService;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ChatSystemMessageService {
    private static final Gson GSON = new Gson();

    private final ConversationRepository conversationRepository;
    private final ConversationMemberRepository memberRepository;
    private final UserIdentityQueryService userIdentityQueryService;
    private final R2dbcEntityTemplate entityTemplate;
    private final ChatResponseMapper mapper;
    private final ChatEventPublisher eventPublisher;

    public Mono<SystemMessageResult> insert(
            String conversationId,
            String actorId,
            SystemMessageAction action,
            String targetUserId,
            String value
    ) {
        return conversationRepository.findByIdForUpdate(conversationId)
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.CONVERSATION_NOT_FOUND, "Conversation not found")))
                .flatMap(conversation -> Mono.zip(identity(actorId), identity(targetUserId))
                        .flatMap(names -> {
                            String actorName = names.getT1();
                            String targetName = names.getT2();
                            long sequence = conversation.getLastMessageSeq() + 1;
                            Instant now = Instant.now();
                            ChatMessage message = ChatMessage.builder()
                                    .id(UUID.randomUUID().toString())
                                    .conversationId(conversationId)
                                    .messageSeq(sequence)
                                    .clientMessageId(UUID.randomUUID().toString())
                                    .senderId(actorId)
                                    .senderDisplayName(actorName)
                                    .messageType(MessageType.SYSTEM)
                                    .content(content(action, actorName, targetName, value))
                                    .metadata(metadata(action, actorId, targetUserId, actorName, targetName, value))
                                    .createdAt(now)
                                    .build();
                            return entityTemplate.insert(ChatMessage.class).using(message)
                                    .flatMap(saved -> conversationRepository.updateMessageSummary(
                                                    conversationId, sequence, saved.getId(), now)
                                            .then(memberRepository.findActiveUserIds(conversationId).collectList())
                                            .map(recipients -> new SystemMessageResult(
                                                    mapper.toChatMessageResponse(saved), recipients)));
                        }));
    }

    public Mono<Void> publish(SystemMessageResult result) {
        return eventPublisher.publish(ChatEvent.messageCreated(result.message(), result.recipientIds()));
    }

    private Mono<String> identity(String userId) {
        if (userId == null || userId.isBlank()) {
            return Mono.just("Thành viên");
        }
        return userIdentityQueryService.resolveDisplayName(userId)
                .map(name -> firstNonBlank(name, userId))
                .defaultIfEmpty(userId);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return "Thành viên";
    }

    private String content(SystemMessageAction action, String actor, String target, String value) {
        return switch (action) {
            case GROUP_CREATED -> actor + " đã tạo nhóm";
            case MEMBER_ADDED -> actor + " đã thêm " + target + " vào nhóm";
            case MEMBER_LEFT -> actor + " đã rời khỏi nhóm";
            case MEMBER_REMOVED -> actor + " đã xóa " + target + " khỏi nhóm";
            case MEMBER_ROLE_CHANGED -> "ADMIN".equals(value)
                    ? actor + " đã đặt " + target + " làm quản trị viên"
                    : actor + " đã gỡ quyền quản trị viên của " + target;
            case NICKNAME_CHANGED -> value == null || value.isBlank()
                    ? actor + " đã xóa biệt danh của " + target
                    : actor + " đã cập nhật biệt danh cho " + target + " là “" + value + "”";
            case GROUP_DISSOLVED -> "Nhóm chat đã bị giải tán";
        };
    }

    private String metadata(
            SystemMessageAction action,
            String actorId,
            String targetUserId,
            String actorName,
            String targetName,
            String value
    ) {
        Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("action", action.name());
        metadata.put("actorId", actorId);
        metadata.put("actorDisplayName", actorName);
        if (targetUserId != null) metadata.put("targetUserId", targetUserId);
        if (targetName != null) metadata.put("targetDisplayName", targetName);
        if (value != null) metadata.put("value", value);
        return GSON.toJson(metadata);
    }

    public record SystemMessageResult(ChatMessageResponse message, List<String> recipientIds) {
    }
}