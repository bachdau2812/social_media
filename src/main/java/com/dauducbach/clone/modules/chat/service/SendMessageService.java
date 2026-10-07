package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.request.MediaMetadataRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.entity.Conversation;
import com.dauducbach.clone.modules.chat.constant.MemberStatus;
import com.dauducbach.clone.modules.chat.repository.ChatMessageRepository;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationMemberRepository;
import com.dauducbach.clone.modules.chat.repository.ConversationRepository;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetRegistry;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

@Service
@RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class SendMessageService {
    private static final Gson GSON = new Gson();

    ChatMessageRepository messageRepository;
    ChatReadRepository chatReadRepository;
    ConversationRepository conversationRepository;
    ConversationMemberRepository memberRepository;
    ChatAccessService accessService;
    ChatMessageValidator validator;
    ChatResponseMapper mapper;
    TransactionalOperator transactionalOperator;
    ChatMessageWriter messageWriter;
    MediaAssetRegistry mediaAssets;
    MessageReactionService reactionService;

    public SendMessageService(ChatMessageRepository messageRepository, ChatReadRepository chatReadRepository,
            ConversationRepository conversationRepository, ConversationMemberRepository memberRepository,
            ChatAccessService accessService, ChatMessageValidator validator, ChatResponseMapper mapper,
            TransactionalOperator transactionalOperator, ChatMessageWriter messageWriter,
            MediaAssetRegistry mediaAssets) {
        this(messageRepository, chatReadRepository, conversationRepository, memberRepository, accessService,
                validator, mapper, transactionalOperator, messageWriter, mediaAssets, null);
    }

    public Mono<ChatMessageResponse> sendMessage(String actorId, String conversationId, SendMessageRequest request) {
        String actor = requireIdentifier(actorId, "actorId");
        String id = requireIdentifier(conversationId, "conversationId");
        ChatMessageValidator.ValidatedMessage validated;
        try {
            validated = validator.validate(request);
        } catch (RuntimeException error) {
            return Mono.error(error);
        }
        String payloadHash = payloadHash(id, request, validated);

        return accessService.requireActiveMember(id, actor)
                .then(messageRepository.findBySenderIdAndClientMessageId(actor, request.clientMessageId())
                        .flatMap(existing -> existingOrConflict(actor, existing, payloadHash, id, request, validated)))
                .switchIfEmpty(Mono.defer(() -> prepareMedia(validated)
                        .flatMap(prepared -> transactionalOperator.transactional(
                                createMessage(actor, id, request, validated, prepared, payloadHash)))
                        .flatMap(message -> presentMessage(actor, message))))
                .onErrorResume(DataIntegrityViolationException.class, error ->
                        messageRepository.findBySenderIdAndClientMessageId(actor, request.clientMessageId())
                                .flatMap(existing -> existingOrConflict(actor, existing, payloadHash, id, request, validated))
                                .switchIfEmpty(Mono.error(error)))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_MESSAGE_CREATE_FAILED, "Create chat message failed", error));
    }

    private Mono<ChatMessageResponse> presentMessage(String actorId, ChatMessage storedMessage) {
        return accessService.requireActiveMember(storedMessage.getConversationId(), actorId)
                .flatMap(member -> {
                    long visibleFrom = Math.max(1L,
                            ChatVisibility.visibleFromSequence(member.getJoinedSeq(), member.getLastDeletedMessageSeq()));
                    if (storedMessage.getMessageSeq() < visibleFrom) {
                        return Mono.error(ChatMessageAccess.forbidden());
                    }
                    return chatReadRepository
                            .findAfterSequence(storedMessage.getConversationId(), visibleFrom,
                                    Math.max(0L, storedMessage.getMessageSeq() - 1L), 1)
                            .next()
                            .defaultIfEmpty(storedMessage)
                            .map(mapper::toChatMessageResponse)
                            .flatMap(response -> {
                                if (reactionService == null || response.deleted()
                                        || response.messageType() == com.dauducbach.clone.modules.chat.constant.MessageType.SYSTEM)
                                    return Mono.just(response);
                                return reactionService.getSnapshots(actorId, response.conversationId(), List.of(response.id()))
                                        .map(snapshots -> snapshots.isEmpty() ? response : response.withReactions(snapshots.getFirst()));
                            });
                });
    }

    private Mono<PreparedMedia> prepareMedia(ChatMessageValidator.ValidatedMessage validated) {
        MediaMetadataRequest requested = validated.metadata();
        if (requested == null) {
            return Mono.just(new PreparedMedia(null, null));
        }
        return mediaAssets.fetchRemoteAsset(requested.publicId().trim())
                .map(media -> new PreparedMedia(media, normalizedMetadata(requested, media)));
    }

    private MediaMetadataRequest normalizedMetadata(MediaMetadataRequest requested, MediaAssetView media) {
        String deliveryUrl = firstNonBlank(media.secureUrl(), media.url(), requested.url());
        long bytes = media.bytes() > 0 ? media.bytes() : requested.size();
        Integer width = requested.width();
        if (media.width() > 0) {
            width = media.width();
        }
        Integer height = requested.height();
        if (media.height() > 0) {
            height = media.height();
        }
        return new MediaMetadataRequest(
                deliveryUrl,
                media.publicId(),
                requested.mimeType(),
                bytes,
                requested.fileName(),
                width,
                height,
                requested.duration());
    }

    private Mono<List<String>> resolveRecipients(
            String actorId,
            SendMessageRequest request,
            List<String> activeMemberIds
    ) {
        if (!activeMemberIds.contains(actorId)) {
            return Mono.error(new AppException(ErrorCode.CONVERSATION_FORBIDDEN, "Active chat membership is required"));
        }
        List<String> actualRecipients = activeMemberIds.stream()
                .filter(userId -> !actorId.equals(userId))
                .distinct()
                .toList();
        if (actualRecipients.isEmpty()) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Conversation has no active recipient"));
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
    }

    private Mono<ChatMessage> createMessage(
            String actorId,
            String conversationId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated,
            PreparedMedia preparedMedia,
            String payloadHash
    ) {
        return conversationRepository.findByIdForUpdate(conversationId)
                .switchIfEmpty(Mono.error(new AppException(
                        ErrorCode.CONVERSATION_NOT_FOUND,
                        "Conversation not found")))
                .flatMap(conversation -> memberRepository.findActiveUserIds(conversationId)
                        .distinct()
                        .sort()
                        .collectList()
                        .flatMap(activeMembers -> resolveRecipients(actorId, request, activeMembers)
                                .flatMap(recipientIds -> messageRepository
                                        .findBySenderIdAndClientMessageId(actorId, request.clientMessageId())
                                        .flatMap(existing -> {
                                            if (!hasSamePayload(existing, payloadHash, conversationId, request, validated)) {
                                                return Mono.error(idempotencyConflict());
                                            }
                                            return Mono.just(existing);
                                        })
                                        .switchIfEmpty(Mono.defer(() -> {
                                            if (conversation.isDissolved()) {
                                                return Mono.error(new AppException(
                                                        ErrorCode.CHAT_CONVERSATION_DISSOLVED,
                                                        "The group conversation is read-only after dissolution"));
                                            }
                                            Mono<Void> replyCheck = request.replyToSeq() == null
                                                    ? Mono.empty()
                                                    : validateReply(actorId, conversationId, request.replyToSeq());
                                            return replyCheck
                                                    .then(insertMessage(actorId, request, validated, preparedMedia,
                                                            payloadHash, conversation, recipientIds));
                                        })))));
    }

    private Mono<Void> validateReply(String actorId, String conversationId, Long replyToSeq) {
        if (replyToSeq == null || replyToSeq <= 0) {
            return Mono.error(new AppException(ErrorCode.CHAT_MESSAGE_REPLY_INVALID, "replyToSeq is invalid"));
        }
        // The conversation lock is already held. Recheck the actor's history boundary inside this transaction.
        return memberRepository.findMembershipForUpdate(conversationId, actorId)
                .filter(member -> member.getMemberStatus() == MemberStatus.ACTIVE)
                .switchIfEmpty(Mono.error(ChatMessageAccess.forbidden()))
                .flatMap(member -> replyToSeq < ChatVisibility.visibleFromSequence(
                        member.getJoinedSeq(), member.getLastDeletedMessageSeq())
                        ? Mono.error(new AppException(ErrorCode.CHAT_MESSAGE_REPLY_INVALID, "Reply message is outside visible history"))
                        : messageRepository.findByConversationIdAndMessageSeq(conversationId, replyToSeq))
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
            String payloadHash,
            Conversation conversation,
            List<String> recipientIds
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
                .clientPayloadHash(payloadHash)
                .senderId(actorId)
                .messageType(request.messageType())
                .content(validated.content())
                .metadata(persistedMetadata(validated, preparedMedia))
                .replyToSeq(request.replyToSeq())
                .createdAt(now)
                .build();

        return messageWriter.write(message, conversation,
                Mono.defer(() -> insertPreparedMedia(preparedMedia.media(), messageId, now)),
                recipientIds);
    }

    private Mono<Void> insertPreparedMedia(MediaAssetView media, String messageId, Instant now) {
        if (media == null) {
            return Mono.empty();
        }
        return mediaAssets.registerFetchedAsset(media, messageId, OwnerType.CHAT_MESSAGE).then();
    }

    private String persistedMetadata(
            ChatMessageValidator.ValidatedMessage validated,
            PreparedMedia preparedMedia
    ) {
        if (validated.storyContext() != null) {
            JsonObject context = new JsonObject();
            context.addProperty("storyId", validated.storyContext().storyId());
            context.addProperty("storyOwnerId", validated.storyContext().storyOwnerId());
            context.addProperty("mediaType", validated.storyContext().mediaType());
            context.addProperty("previewAtMs", validated.storyContext().previewAtMs());
            context.addProperty("expiresAt", validated.storyContext().expiresAt().toString());
            return context.toString();
        }
        return preparedMedia.metadata() == null ? null : GSON.toJson(preparedMedia.metadata());
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value;
            }
        }
        return null;
    }

    private Mono<ChatMessageResponse> existingOrConflict(
            String actorId,
            ChatMessage existing,
            String payloadHash,
            String conversationId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated
    ) {
        if (!hasSamePayload(existing, payloadHash, conversationId, request, validated)) {
            return Mono.error(idempotencyConflict());
        }
        return presentMessage(actorId, existing);
    }

    private boolean hasSamePayload(
            ChatMessage existing,
            String payloadHash,
            String conversationId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated
    ) {
        if (existing.getClientPayloadHash() != null) {
            return Objects.equals(payloadHash, existing.getClientPayloadHash());
        }
        return matchesLegacyPayload(existing, conversationId, request, validated);
    }

    private AppException idempotencyConflict() {
        return new AppException(
                ErrorCode.CHAT_MESSAGE_IDEMPOTENCY_CONFLICT,
                "clientMessageId was already accepted for a different conversation or payload");
    }

    private boolean matchesLegacyPayload(
            ChatMessage existing,
            String conversationId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated
    ) {
        if (!Objects.equals(existing.getConversationId(), conversationId)
                || existing.getMessageType() != request.messageType()
                || !Objects.equals(existing.getContent(), validated.content())
                || !Objects.equals(existing.getReplyToSeq(), request.replyToSeq())) {
            return false;
        }
        if (validated.storyContext() != null) {
            return Objects.equals(existing.getMetadata(), persistedMetadata(validated, null));
        }
        if (validated.metadata() == null) {
            return existing.getMetadata() == null;
        }
        if (existing.getMetadata() == null) {
            return false;
        }
        try {
            JsonObject stored = JsonParser.parseString(existing.getMetadata()).getAsJsonObject();
            MediaMetadataRequest requested = validated.metadata();
            return Objects.equals(storedString(stored, "publicId"), requested.publicId().trim())
                    && Objects.equals(storedString(stored, "mimeType"), requested.mimeType())
                    && Objects.equals(storedString(stored, "fileName"), requested.fileName());
        } catch (RuntimeException error) {
            return false;
        }
    }

    private String storedString(JsonObject object, String name) {
        return object.has(name) && !object.get(name).isJsonNull() ? object.get(name).getAsString() : null;
    }

    private String payloadHash(
            String conversationId,
            SendMessageRequest request,
            ChatMessageValidator.ValidatedMessage validated
    ) {
        var story = validated.storyContext();
        String payload = GSON.toJson(new IdempotencyPayload(
                conversationId,
                request.messageType(),
                validated.content(),
                validated.metadata(),
                request.replyToSeq(),
                story == null ? null : story.storyId(),
                story == null ? null : story.storyOwnerId(),
                story == null ? null : story.mediaType(),
                story == null ? null : story.previewAtMs(),
                story == null || story.expiresAt() == null ? null : story.expiresAt().toString()));
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(payload.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is not available", error);
        }
    }

    private String requireIdentifier(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.CHAT_REQUEST_INVALID, name + " is required");
        }
        return value.trim();
    }

    private record PreparedMedia(MediaAssetView media, MediaMetadataRequest metadata) {
    }

    private record IdempotencyPayload(
            String conversationId,
            com.dauducbach.clone.modules.chat.constant.MessageType messageType,
            String content,
            MediaMetadataRequest metadata,
            Long replyToSeq,
            String storyId,
            String storyOwnerId,
            String storyMediaType,
            Long storyPreviewAtMs,
            String storyExpiresAt
    ) {
    }
}
