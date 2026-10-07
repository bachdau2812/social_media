package com.dauducbach.clone.integration.storychat;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.publicapi.StoryReplySender;
import com.dauducbach.clone.modules.post.publicapi.StoryQuery;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
public class StoryReplyUseCase {
    private final StoryQuery storyQuery;
    private final StoryReplySender storyReplySender;

    public Mono<StoryReplyResponse> reply(String storyId, String authenticatedUserId, StoryReplyRequest request) {
        String senderId = required(authenticatedUserId, "authenticatedUserId");
        String cleanStoryId = required(storyId, "storyId");
        if (request == null) {
            return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Story reply request is required"));
        }
        if (request.previewAtMs() == null || request.previewAtMs() < 0) {
            return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Story preview time is invalid"));
        }

        StoryQuery.StoryReference reference = new StoryQuery.StoryReference(cleanStoryId, request.previewAtMs());
        Instant now = Instant.now();
        return storyQuery.resolve(List.of(reference), now)
                .flatMap(resolved -> {
                    StoryQuery.StoryAvailability story = resolved.get(reference);
                    if (story == null) {
                        return Mono.error(new AppException(ErrorCode.STORY_NOT_FOUND, "Story not found"));
                    }
                    if (!story.available() || story.expiresAt() == null || !story.expiresAt().isAfter(now)) {
                        return Mono.error(new AppException(ErrorCode.STORY_NOT_FOUND, "Story is unavailable"));
                    }
                    if (senderId.equals(story.ownerId())) {
                        return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Cannot reply to your own Story"));
                    }
                    String mediaType = story.mediaType() == null ? "" : story.mediaType().trim().toUpperCase(Locale.ROOT);
                    if (!("VIDEO".equals(mediaType) || "IMAGE".equals(mediaType))
                            || ("IMAGE".equals(mediaType) && request.previewAtMs() != 0)) {
                        return Mono.error(new AppException(ErrorCode.STORY_SAVE_FAILED, "Story preview time is invalid"));
                    }
                    StoryReplySender.StoryReplyCommand command = new StoryReplySender.StoryReplyCommand(
                            senderId,
                            story.storyId(),
                            story.ownerId(),
                            request.content(),
                            request.clientMessageId(),
                            mediaType,
                            request.previewAtMs(),
                            story.expiresAt());
                    return storyReplySender.send(command)
                            .map(message -> new StoryReplyResponse(
                                    message.conversationId(), message.messageId(), message.messageSeq()));
                });
    }

    private String required(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new AppException(ErrorCode.STORY_SAVE_FAILED, field + " is required");
        }
        return value.trim();
    }
}
