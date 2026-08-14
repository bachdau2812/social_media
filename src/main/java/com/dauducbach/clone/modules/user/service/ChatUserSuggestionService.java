package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.user.dto.response.ChatUserSuggestionResponse;
import com.dauducbach.clone.modules.user.repositoty.ChatUserSuggestionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class ChatUserSuggestionService {
    private static final int DEFAULT_LIMIT = 30;
    private static final int MAX_LIMIT = 50;

    private final ChatUserSuggestionRepository repository;

    public Flux<ChatUserSuggestionResponse> getSuggestions(String viewerId, String query, int limit) {
        if (viewerId == null || viewerId.isBlank()) {
            return Flux.error(new AppException(ErrorCode.SEARCH_REQUEST_INVALID, "viewerId is required"));
        }
        String normalizedQuery = query == null ? "" : query.trim();
        String queryPattern = normalizedQuery.isEmpty() ? "" : normalizedQuery + "%";
        int normalizedLimit = limit <= 0 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
        return repository.findSuggestions(viewerId.trim(), queryPattern, normalizedLimit)
                .onErrorMap(error -> error instanceof AppException
                        ? error
                        : new AppException(ErrorCode.USER_SEARCH_FAILED, "Fetch chat user suggestions failed", error));
    }
}
