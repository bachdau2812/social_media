package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.CursorPageResponse;
import com.dauducbach.clone.modules.chat.dto.response.StoryContextResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import com.dauducbach.clone.modules.media.publicapi.MediaAssetView;
import com.dauducbach.clone.modules.media.publicapi.MediaCatalog;
import com.dauducbach.clone.modules.post.publicapi.StoryQuery;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Map;

@Service
@RequiredArgsConstructor(onConstructor_ = @org.springframework.beans.factory.annotation.Autowired)
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ChatMessageQueryService {
    static final int DEFAULT_PAGE_SIZE = 50;
    static final int MAX_PAGE_SIZE = 100;

    ChatAccessService accessService;
    ChatReadRepository chatReadRepository;
    ChatResponseMapper mapper;
    StoryQuery storyQuery;
    MessageReactionService reactionService;
    UserIdentityQuery userIdentityQuery;
    MediaCatalog mediaCatalog;

    public ChatMessageQueryService(ChatAccessService accessService, ChatReadRepository chatReadRepository,
            ChatResponseMapper mapper, StoryQuery storyQuery) {
        this(accessService, chatReadRepository, mapper, storyQuery, null, null, null);
    }

    public ChatMessageQueryService(ChatAccessService accessService, ChatReadRepository chatReadRepository,
            ChatResponseMapper mapper, StoryQuery storyQuery, MessageReactionService reactionService) {
        this(accessService, chatReadRepository, mapper, storyQuery, reactionService, null, null);
    }

    public Mono<CursorPageResponse<ChatMessageResponse>> getMessages(
            String actorId,
            String conversationId,
            Long afterSeq,
            Long beforeSeq,
            int limit
    ) {
        if (afterSeq != null && beforeSeq != null) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Use either afterSeq or beforeSeq"));
        }
        int pageSize = normalizeLimit(limit);
        boolean backward = afterSeq == null;
        return accessService.requireActiveMember(conversationId, actorId)
                .flatMapMany(member -> {
                    long visibleFrom = ChatVisibility.visibleFromSequence(
                            member.getJoinedSeq(), member.getLastDeletedMessageSeq());
                    return afterSeq != null
                            ? chatReadRepository.findAfterSequence(conversationId, visibleFrom, afterSeq, pageSize + 1)
                            : chatReadRepository.findBeforeSequence(
                                    conversationId,
                                    visibleFrom,
                                    beforeSeq == null ? Long.MAX_VALUE : beforeSeq,
                                    pageSize + 1);
                })
                .collectList()
                .flatMap(rows -> hydrateMessageProfiles(rows).map(hydrated -> toCursorPage(hydrated, pageSize, backward)))
                .flatMap(this::hydrateStoryAvailability)
                .flatMap(page -> hydrateReactions(actorId, conversationId, page))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_MESSAGE_FETCH_FAILED, "Fetch chat messages failed", error));
    }

    /** Shared REST hydration for history, independent pins and batch reconnect state; does not advance cursors. */
    public Mono<List<ChatMessageResponse>> hydrateMessages(String actorId,String conversationId,List<ChatMessageResponse> messages) {
        return hydrateStoryAvailability(new CursorPageResponse<>(messages,null,false))
            .flatMap(page->hydrateReactions(actorId,conversationId,page)).map(CursorPageResponse::items);
    }

    /** Batch-reads owner snapshots once per bounded message page; profile failure keeps chat rows readable. */
    private Mono<List<ChatMessage>> hydrateMessageProfiles(List<ChatMessage> messages) {
        if (messages.isEmpty() || userIdentityQuery == null || mediaCatalog == null) return Mono.just(messages);
        List<String> userIds = messages.stream()
                .flatMap(message -> java.util.stream.Stream.of(message.getSenderId(), message.getReplySenderId()))
                .filter(id -> id != null && !id.isBlank())
                .distinct()
                .toList();
        if (userIds.isEmpty()) return Mono.just(messages);
        Mono<Map<String, UserIdentity>> identities = userIdentityQuery.findIdentities(userIds)
                .collectMap(UserIdentity::userId)
                .onErrorReturn(Map.of());
        Mono<Map<String, MediaAssetView>> avatars = mediaCatalog.findCurrentAvatars(userIds)
                .collectMap(MediaAssetView::ownerId)
                .onErrorReturn(Map.of());
        return Mono.zip(identities, avatars).map(snapshots -> {
            Map<String, UserIdentity> identityById = snapshots.getT1();
            Map<String, MediaAssetView> avatarByOwner = snapshots.getT2();
            messages.forEach(message -> {
                UserIdentity sender = identityById.get(message.getSenderId());
                message.setSenderDisplayName(displayName(message.getSenderDisplayName(), sender, message.getSenderId()));
                MediaAssetView avatar = avatarByOwner.get(message.getSenderId());
                message.setSenderAvatarUrl(avatar == null ? null : firstNonBlank(avatar.secureUrl(), avatar.url()));
                if (message.getReplySenderId() != null) {
                    UserIdentity replySender = identityById.get(message.getReplySenderId());
                    message.setReplySenderDisplayName(displayName(
                            message.getReplySenderDisplayName(), replySender, message.getReplySenderId()));
                }
            });
            return messages;
        });
    }

    private String displayName(String chatNickname, UserIdentity identity, String fallbackId) {
        return firstNonBlank(chatNickname,
                identity == null ? null : identity.fullName(),
                identity == null ? null : identity.username(),
                fallbackId);
    }

    private String firstNonBlank(String... values) {
        for (String value : values) if (value != null && !value.isBlank()) return value;
        return null;
    }

    private Mono<CursorPageResponse<ChatMessageResponse>> hydrateReactions(
            String actorId, String conversationId, CursorPageResponse<ChatMessageResponse> page) {
        if (reactionService == null) return Mono.just(page); // Legacy Java constructor only.
        List<String> ids = page.items().stream()
                .filter(message -> !message.deleted()
                        && message.messageType() != com.dauducbach.clone.modules.chat.constant.MessageType.SYSTEM)
                .map(ChatMessageResponse::id).toList();
        if (ids.isEmpty()) return Mono.just(page);
        return reactor.core.publisher.Flux.fromIterable(ids).buffer(MAX_PAGE_SIZE)
                .concatMap(batch -> reactionService.getSnapshots(actorId, conversationId, batch))
                .flatMapIterable(snapshots -> snapshots).collectList().map(snapshots -> {
            var byId = snapshots.stream().collect(java.util.stream.Collectors.toMap(
                    com.dauducbach.clone.modules.chat.dto.response.ReactionSnapshot::messageId, snapshot -> snapshot));
            return new CursorPageResponse<>(page.items().stream().map(message -> {
                var snapshot = byId.get(message.id());
                return snapshot == null ? message : message.withReactions(snapshot);
            }).toList(), page.nextCursor(), page.hasMore());
        });
    }

    private Mono<CursorPageResponse<ChatMessageResponse>> hydrateStoryAvailability(
            CursorPageResponse<ChatMessageResponse> page
    ) {
        var references = page.items().stream()
                .map(ChatMessageResponse::storyContext)
                .filter(java.util.Objects::nonNull)
                .map(context -> new StoryQuery.StoryReference(
                        context.storyId(), context.previewAtMs() == null ? 0L : context.previewAtMs()))
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        if (references.isEmpty()) {
            return Mono.just(page);
        }
        return storyQuery.resolve(references, Instant.now())
                .map(resolved -> new CursorPageResponse<>(
                        page.items().stream()
                                .map(message -> hydrateStoryContext(message, resolved))
                                .toList(),
                        page.nextCursor(),
                        page.hasMore()));
    }

    private ChatMessageResponse hydrateStoryContext(
            ChatMessageResponse message,
            java.util.Map<StoryQuery.StoryReference, StoryQuery.StoryAvailability> resolved
    ) {
        StoryContextResponse context = message.storyContext();
        if (context == null) {
            return message;
        }
        StoryQuery.StoryReference reference = new StoryQuery.StoryReference(
                context.storyId(), context.previewAtMs() == null ? 0L : context.previewAtMs());
        StoryQuery.StoryAvailability availability = resolved.get(reference);
        StoryContextResponse hydrated = availability == null
                ? new StoryContextResponse(
                        context.storyId(), context.storyOwnerId(), context.mediaType(), context.previewAtMs(),
                        context.expiresAt(), false, null)
                : new StoryContextResponse(
                        context.storyId(), context.storyOwnerId(), availability.mediaType(), availability.previewAtMs(),
                        availability.expiresAt(), availability.available(), availability.previewUrl());
        return new ChatMessageResponse(
                message.id(), message.conversationId(), message.messageSeq(), message.clientMessageId(),
                message.senderId(), message.senderDisplayName(), message.senderAvatarUrl(),
                message.messageType(), message.content(), message.metadata(), message.replyToSeq(), message.reply(),
                message.createdAt(), message.editedAt(), message.deleted(), hydrated,
                message.likeCount(), message.isReact(), message.myReaction(), message.reactions(), message.reactionVersion(), message.forwarded());
    }

    private CursorPageResponse<ChatMessageResponse> toCursorPage(List<ChatMessage> rows, int pageSize, boolean backward) {
        List<ChatMessage> pageRows = new ArrayList<>(rows);
        boolean hasMore = pageRows.size() > pageSize;
        if (hasMore) {
            pageRows.remove(backward ? 0 : pageRows.size() - 1);
        }
        List<ChatMessageResponse> items = pageRows.stream()
                .map(mapper::toChatMessageResponse)
                .toList();
        String nextCursor = null;
        if (hasMore && !pageRows.isEmpty()) {
            ChatMessage edge = backward ? pageRows.getFirst() : pageRows.getLast();
            nextCursor = String.valueOf(edge.getMessageSeq());
        }
        return new CursorPageResponse<>(items, nextCursor, hasMore);
    }

    private int normalizeLimit(int requestedLimit) {
        if (requestedLimit <= 0) {
            return DEFAULT_PAGE_SIZE;
        }
        return Math.min(requestedLimit, MAX_PAGE_SIZE);
    }
}
