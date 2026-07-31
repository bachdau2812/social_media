package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.dto.response.ChatMessageResponse;
import com.dauducbach.clone.modules.chat.dto.response.CursorPageResponse;
import com.dauducbach.clone.modules.chat.dto.response.StoryContextResponse;
import com.dauducbach.clone.modules.chat.entity.ChatMessage;
import com.dauducbach.clone.modules.chat.repository.ChatReadRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.time.Instant;
import java.util.LinkedHashSet;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class ChatMessageQueryService {
    static final int DEFAULT_PAGE_SIZE = 50;
    static final int MAX_PAGE_SIZE = 100;

    ChatAccessService accessService;
    ChatReadRepository chatReadRepository;
    ChatResponseMapper mapper;
    ChatCursorService cursorService;
    StoryAvailabilityPort storyAvailabilityPort;

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
                .map(rows -> toCursorPage(rows, pageSize, backward))
                .flatMap(this::hydrateStoryAvailability)
                .flatMap(page -> markFetchedMessagesDelivered(actorId, conversationId, page))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_MESSAGE_FETCH_FAILED, "Fetch chat messages failed", error));
    }

    private Mono<CursorPageResponse<ChatMessageResponse>> hydrateStoryAvailability(
            CursorPageResponse<ChatMessageResponse> page
    ) {
        var references = page.items().stream()
                .map(ChatMessageResponse::storyContext)
                .filter(java.util.Objects::nonNull)
                .map(context -> new StoryAvailabilityPort.StoryReference(
                        context.storyId(), context.previewAtMs() == null ? 0L : context.previewAtMs()))
                .collect(java.util.stream.Collectors.toCollection(LinkedHashSet::new));
        if (references.isEmpty()) {
            return Mono.just(page);
        }
        return storyAvailabilityPort.resolve(references, Instant.now())
                .map(resolved -> new CursorPageResponse<>(
                        page.items().stream()
                                .map(message -> hydrateStoryContext(message, resolved))
                                .toList(),
                        page.nextCursor(),
                        page.hasMore()));
    }

    private ChatMessageResponse hydrateStoryContext(
            ChatMessageResponse message,
            java.util.Map<StoryAvailabilityPort.StoryReference, StoryAvailabilityPort.StoryAvailability> resolved
    ) {
        StoryContextResponse context = message.storyContext();
        if (context == null) {
            return message;
        }
        StoryAvailabilityPort.StoryReference reference = new StoryAvailabilityPort.StoryReference(
                context.storyId(), context.previewAtMs() == null ? 0L : context.previewAtMs());
        StoryAvailabilityPort.StoryAvailability availability = resolved.get(reference);
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
                message.createdAt(), message.editedAt(), message.deleted(), hydrated);
    }

    private Mono<CursorPageResponse<ChatMessageResponse>> markFetchedMessagesDelivered(
            String actorId,
            String conversationId,
            CursorPageResponse<ChatMessageResponse> page
    ) {
        long deliveredSequence = page.items().stream()
                .mapToLong(ChatMessageResponse::messageSeq)
                .max()
                .orElse(0L);
        if (deliveredSequence <= 0) {
            return Mono.just(page);
        }
        return cursorService.markDelivered(actorId, conversationId, deliveredSequence)
                .thenReturn(page);
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
