package com.dauducbach.clone.integration.storychat;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.publicapi.StoryReplySender;
import com.dauducbach.clone.modules.post.publicapi.StoryQuery;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StoryReplyUseCaseTest {
    @Test
    void validatesStoryThenSendsTheReplyThroughChatPublicContract() {
        StoryQuery stories = mock(StoryQuery.class);
        StoryReplySender chat = mock(StoryReplySender.class);
        StoryReplyUseCase useCase = new StoryReplyUseCase(stories, chat);
        StoryReplyRequest request = request(12400L);
        StoryQuery.StoryReference reference = new StoryQuery.StoryReference("story-1", 12400L);
        Instant expiry = Instant.now().plusSeconds(3600);
        when(stories.resolve(eq(List.of(reference)), any(Instant.class))).thenReturn(Mono.just(Map.of(
                reference, new StoryQuery.StoryAvailability(
                        "story-1", "owner-1", true, "VIDEO", 12400L, expiry, "https://cdn/still.jpg"))));
        when(chat.send(any())).thenReturn(Mono.just(
                new StoryReplySender.StoryReplyMessage("conversation-1", "message-1", 9)));

        StepVerifier.create(useCase.reply("story-1", " sender-1 ", request))
                .expectNext(new StoryReplyResponse("conversation-1", "message-1", 9))
                .verifyComplete();

        verify(chat).send(new StoryReplySender.StoryReplyCommand(
                "sender-1", "story-1", "owner-1", "hello", request.clientMessageId(), "VIDEO", 12400L, expiry));
    }

    @Test
    void blocksUnavailableOwnAndInvalidPreviewStoriesBeforeSending() {
        StoryQuery stories = mock(StoryQuery.class);
        StoryReplySender chat = mock(StoryReplySender.class);
        StoryReplyUseCase useCase = new StoryReplyUseCase(stories, chat);
        Instant expiry = Instant.now().plusSeconds(600);
        StoryQuery.StoryReference ownRef = new StoryQuery.StoryReference("own", 0);
        StoryQuery.StoryReference removedRef = new StoryQuery.StoryReference("removed", 0);
        StoryQuery.StoryReference imageRef = new StoryQuery.StoryReference("image", 1000);
        when(stories.resolve(eq(List.of(ownRef)), any(Instant.class))).thenReturn(Mono.just(Map.of(
                ownRef, new StoryQuery.StoryAvailability("own", "sender-1", true, "IMAGE", 0, expiry, "url"))));
        when(stories.resolve(eq(List.of(removedRef)), any(Instant.class))).thenReturn(Mono.just(Map.of(
                removedRef, new StoryQuery.StoryAvailability("removed", "owner-1", false, "IMAGE", 0, expiry, null))));
        when(stories.resolve(eq(List.of(imageRef)), any(Instant.class))).thenReturn(Mono.just(Map.of(
                imageRef, new StoryQuery.StoryAvailability("image", "owner-1", true, "IMAGE", 1000, expiry, "url"))));

        assertError(useCase.reply("own", "sender-1", request(0)), ErrorCode.STORY_SAVE_FAILED);
        assertError(useCase.reply("removed", "sender-1", request(0)), ErrorCode.STORY_NOT_FOUND);
        assertError(useCase.reply("image", "sender-1", request(1000)), ErrorCode.STORY_SAVE_FAILED);
        verify(chat, never()).send(any());
    }

    private StoryReplyRequest request(long previewAtMs) {
        return new StoryReplyRequest("hello", UUID.randomUUID().toString(), previewAtMs);
    }

    private void assertError(Mono<?> result, ErrorCode expected) {
        StepVerifier.create(result)
                .expectErrorSatisfies(error -> {
                    assertThat(error).isInstanceOf(AppException.class);
                    assertThat(((AppException) error).getErrorCode()).isEqualTo(expected);
                })
                .verify();
    }
}
