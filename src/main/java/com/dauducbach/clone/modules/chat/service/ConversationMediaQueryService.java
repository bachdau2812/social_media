package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.ChatMediaCategory;
import com.dauducbach.clone.modules.chat.dto.response.ChatMediaResponse;
import com.dauducbach.clone.modules.chat.dto.response.CursorPageResponse;
import com.dauducbach.clone.modules.chat.repository.ConversationMediaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
public class ConversationMediaQueryService {
    private static final int DEFAULT_LIMIT = 30;
    private static final int MAX_LIMIT = 60;

    private final ChatAccessService accessService;
    private final ConversationMediaRepository mediaRepository;
    private final ChatResponseMapper mapper;

    public Mono<CursorPageResponse<ChatMediaResponse>> getMedia(
            String actorId,
            String conversationId,
            ChatMediaCategory category,
            Long beforeSeq,
            int requestedLimit
    ) {
        int limit = requestedLimit <= 0 ? DEFAULT_LIMIT : Math.min(requestedLimit, MAX_LIMIT);
        long cursor = beforeSeq == null ? Long.MAX_VALUE : beforeSeq;
        if (cursor <= 0) {
            return Mono.error(new AppException(ErrorCode.CHAT_REQUEST_INVALID, "beforeSeq must be positive"));
        }
        ChatMediaCategory selectedCategory = category == null ? ChatMediaCategory.IMAGE : category;
        return accessService.requireActiveMember(conversationId, actorId)
                .flatMapMany(member -> mediaRepository.findMedia(
                        conversationId,
                        ChatVisibility.visibleFromSequence(
                                member.getJoinedSeq(), member.getLastDeletedMessageSeq()),
                        selectedCategory,
                        cursor,
                        limit + 1))
                .collectList()
                .map(rows -> toPage(rows, limit))
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.CHAT_MESSAGE_FETCH_FAILED, "Fetch conversation media failed", error));
    }

    private CursorPageResponse<ChatMediaResponse> toPage(
            List<ConversationMediaRepository.ConversationMediaRow> rows,
            int limit
    ) {
        List<ConversationMediaRepository.ConversationMediaRow> pageRows = new ArrayList<>(rows);
        boolean hasMore = pageRows.size() > limit;
        if (hasMore) pageRows.remove(pageRows.size() - 1);
        List<ChatMediaResponse> items = pageRows.stream()
                .map(row -> new ChatMediaResponse(
                        row.messageId(), row.messageSeq(), row.messageType(),
                        mapper.toMediaMetadataResponse(row.metadata()), row.createdAt()))
                .toList();
        String nextCursor = hasMore && !pageRows.isEmpty()
                ? String.valueOf(pageRows.get(pageRows.size() - 1).messageSeq())
                : null;
        return new CursorPageResponse<>(items, nextCursor, hasMore);
    }
}