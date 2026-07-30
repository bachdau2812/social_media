package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.event.ChatEvent;
import com.dauducbach.clone.modules.chat.dto.request.MediaMetadataRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.repository.ChatMessageRepository;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.media.service.MediaCompatibilityFacade;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class SendMessageService {
    private static final Logger log = LoggerFactory.getLogger(SendMessageService.class);
    private static final Gson GSON = new Gson();

    ChatMessageRepository messageRepository;
    ChatReadRepository chatReadRepository;
    ConversationRepository conversationRepository;
    ConversationMemberRepository memberRepository;
    ChatAccessService accessService;
    ChatMessageValidator validator;
    ChatResponseMapper mapper;
    ChatEventPublisher eventPublisher;
    TransactionalOperator transactionalOperator;
    R2dbcEntityTemplate r2dbcEntityTemplate;
    MediaCompatibilityFacade cloudinaryMediaService;
    MediaService mediaService;

    public Mono<ChatMessageResponse> sendMessage(String actorId, String conversationId, SendMessageRequest request) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        ChatMessageValidator.ValidatedMessage validated;
        try {
            validated = validator.validate(request);
        } catch (RuntimeException error) {
            return Mono.error(error);
        }

        return resolveRecipients(id, actor, request)
                .flatMap(recipientIds -> messageRepository
                        .findBySenderIdAndClientMessageId(actor, request.clientMessageId())
                        .flatMap(existing -> presentMessage(existing, false, recipientIds))
                        .switchIfEmpty(Mono.defer(() -> prepareMedia(validated)
                                .flatMap(prepared -> transactionalOperator.transactional(
                                        createMessage(actor, id, request, validated, prepared)))
                                .flatMap(created -> presentMessage(created, true, recipientIds)))))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_MESSAGE_CREATE_FAILED, "Create chat message failed", error));
    }

    private Mono<ChatMessageResponse> presentMessage(
            ChatMessage storedMessage,
            boolean created,
            List<String> recipientIds
    ) {
        return chatReadRepository
                .findAfterSequence(
                        storedMessage.getConversationId(),
                        1L,
                        Math.max(0L, storedMessage.getMessageSeq() - 1L),
                        1)
                .next()
                .defaultIfEmpty(storedMessage)
                .flatMap(presentedMessage -> {
                    ChatMessageResponse response = mapper.toChatMessageResponse(presentedMessage);
                    if (!created) {
                        return Mono.just(response);
                    }
                    return eventPublisher.publish(ChatEvent.messageCreated(response, recipientIds))
                            .retryWhen(Retry.backoff(3, Duration.ofMillis(150)))
                            .onErrorResume(error -> {
                                log.error("|SendMessageService|sendMessage|event publish failed after commit|conversationId={}|messageId={}|error={}",
                                        response.conversationId(), response.id(), error.getMessage());
                                return Mono.empty();
                            })
                            .thenReturn(response);
                });
    }

    private Mono<PreparedMedia> prepareMedia(ChatMessageValidator.ValidatedMessage validated) {
        MediaMetadataRequest requested = validated.metadata();
        if (requested == null) {
            return Mono.just(new PreparedMedia(null, null));
        }
        return cloudinaryMediaService.fetchMediaByPublicId(requested.publicId().trim())
                .map(media -> new PreparedMedia(media, normalizedMetadata(requested, media)));
    }

    private MediaMetadataRequest normalizedMetadata(MediaMetadataRequest requested, Media media) {
        String deliveryUrl = firstNonBlank(media.getSecureUrl(), media.getUrl(), requested.url());
        long bytes = media.getBytes() > 0 ? media.getBytes() : requested.size();
        Integer width = media.getWidth() > 0 ? media.getWidth() : requested.width();
        Integer height = media.getHeight() > 0 ? media.getHeight() : requested.height();
        return new MediaMetadataRequest(
                deliveryUrl,
                media.getPublicId(),
                requested.mimeType(),
                bytes,
                requested.fileName(),
                width,
                height,
                requested.duration());
    }

    private Mono<List<String>> resolveRecipients(
            String conversationId,
            String actorId,
            SendMessageRequest request
    ) {
        return accessService.requireActiveMember(conversationId, actorId)
                .thenMany(memberRepository.findActiveUserIds(conversationId))
                .filter(userId -> !actorId.equals(userId))
                .distinct()
                .collectList()
                .flatMap(actualRecipients -> {
                    if (actualRecipients.isEmpty()) {
                        return Mono.error(new AppException(
                                ErrorCode.CHAT_REQUEST_INVALID,
                                "Conversation has no active recipient"));
                    }

                    Set<String> suppliedRecipients = new LinkedHashSet<>();
                    if (request.recipientId() != null && !request.recipientId().isBlank()) {
                        suppliedRecipients.add(request.recipientId().trim());
                    }
                    if (request.recipientIds() != null) {
                        request.recipientIds().stream()
                                .filter(value -> value != null && !value.isBlank())
                                .map(String::trim)
                                .forEach(suppliedRecipients::add);
                    }

                    if (!suppliedRecipients.isEmpty()
                            && !suppliedRecipients.equals(new LinkedHashSet<>(actualRecipients))) {
                        return Mono.error(new AppException(
                                ErrorCode.CHAT_REQUEST_INVALID,
                                "recipientId does not match active conversation members"));
                    }
                    return Mono.just(List.copyOf(actualRecipients));
                });
    }

    private Mono<ChatMessage> createMessage(
            String actorId,
            String conversationId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated,
            PreparedMedia preparedMedia
    ) {
        Mono<Void> replyCheck = request.replyToSeq() == null
                ? Mono.empty()
                : validateReply(conversationId, request.replyToSeq());

        return replyCheck
                .then(conversationRepository.findByIdForUpdate(conversationId)
                        .switchIfEmpty(Mono.error(new AppException(
                                ErrorCode.CONVERSATION_NOT_FOUND,
                                "Conversation not found"))))
                .flatMap(conversation -> conversation.isDissolved()
                        ? Mono.error(new AppException(
                                ErrorCode.CHAT_CONVERSATION_DISSOLVED,
                                "The group conversation is read-only after dissolution"))
                        : insertMessage(actorId, request, validated, preparedMedia, conversation));
    }

    private Mono<Void> validateReply(String conversationId, Long replyToSeq) {
        if (replyToSeq == null || replyToSeq <= 0) {
            return Mono.error(new AppException(ErrorCode.CHAT_MESSAGE_REPLY_INVALID, "replyToSeq is invalid"));
        }
        return messageRepository.findByConversationIdAndMessageSeq(conversationId, replyToSeq)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CHAT_MESSAGE_REPLY_INVALID,
                        "Reply message was not found")))
                .then();
    }

    private Mono<ChatMessage> insertMessage(
            String actorId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated,
            PreparedMedia preparedMedia,
            Conversation conversation
    ) {
        long messageSeq = conversation.getLastMessageSeq() + 1;
        if (request.replyToSeq() != null && request.replyToSeq() >= messageSeq) {
            return Mono.error(new AppException(ErrorCode.CHAT_MESSAGE_REPLY_INVALID, "replyToSeq must be before the new message"));
        }

        Instant now = Instant.now();
        String messageId = UUID.randomUUID().toString();
        ChatMessage message = ChatMessage.builder()
                .id(messageId)
                .conversationId(conversation.getId())
                .messageSeq(messageSeq)
                .clientMessageId(request.clientMessageId())
                .senderId(actorId)
                .messageType(request.messageType())
                .content(validated.content())
                .metadata(preparedMedia.metadata() == null ? null : GSON.toJson(preparedMedia.metadata()))
                .replyToSeq(request.replyToSeq())
                .createdAt(now)
                .build();

        return r2dbcEntityTemplate.insert(ChatMessage.class).using(message)
                .flatMap(saved -> insertPreparedMedia(preparedMedia.media(), messageId, now)
                        .then(conversationRepository.updateMessageSummary(
                                conversation.getId(), messageSeq, saved.getId(), now))
                        .thenReturn(saved));
    }

    private Mono<Void> insertPreparedMedia(Media media, String messageId, Instant now) {
        if (media == null) {
            return Mono.empty();
        }
        return mediaService.registerFetchedMedia(media, messageId, OwnerType.CHAT_MESSAGE).then();
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private String requireIdentifier(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.CHAT_REQUEST_INVALID, name + " is required");
        }
        return value.trim();
    }

    private record PreparedMedia(Media media, MediaMetadataRequest metadata) {
    }
}
