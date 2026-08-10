package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.MediaMetadataRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.request.StoryContextRequest;
import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.util.unit.DataSize;

import java.util.UUID;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChatMessageValidatorTest {

    private final ChatMessageValidator validator = new ChatMessageValidator(mediaPolicy());

    @Test
    void configuredImageAndVideoLimitsAcceptBoundaryAndRejectOneByteOver() {
        long limit = 100L * 1024 * 1024;

        assertThat(validator.validate(mediaRequest(
                MessageType.IMAGE,
                new MediaMetadataRequest("https://host/image.jpg", "image", "image/jpeg", limit, "image.jpg", 1, 1, null))))
                .isNotNull();
        assertThat(validator.validate(mediaRequest(
                MessageType.VIDEO,
                new MediaMetadataRequest("https://host/video.mp4", "video", "video/mp4", limit, "video.mp4", 1, 1, 1L))))
                .isNotNull();
        assertChatError(mediaRequest(
                MessageType.IMAGE,
                new MediaMetadataRequest("https://host/image.jpg", "image", "image/jpeg", limit + 1, "image.jpg", 1, 1, null)),
                ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
        assertChatError(mediaRequest(
                MessageType.VIDEO,
                new MediaMetadataRequest("https://host/video.mp4", "video", "video/mp4", limit + 1, "video.mp4", 1, 1, 1L)),
                ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void configuredAudioLimitRemainsFiftyMegabytes() {
        long limit = 50L * 1024 * 1024;

        assertThat(validator.validate(mediaRequest(
                MessageType.AUDIO,
                new MediaMetadataRequest("https://host/audio.mp3", "audio", "audio/mpeg", limit, "audio.mp3", null, null, 1L))))
                .isNotNull();
        assertChatError(mediaRequest(
                MessageType.AUDIO,
                new MediaMetadataRequest("https://host/audio.mp3", "audio", "audio/mpeg", limit + 1, "audio.mp3", null, null, 1L)),
                ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void textMessageDecodesHtmlEntitiesIntoPlainText() {
        ChatMessageValidator.ValidatedMessage validated = validator.validate(textRequest("Tom &amp; Jerry<script>alert('x')</script>"));

        assertThat(validated.content()).isEqualTo("Tom & Jerry");
    }

    @Test
    void textMessageKeepsWhitespaceBetweenBlockAndBreakBoundaries() {
        ChatMessageValidator.ValidatedMessage validated = validator.validate(
                textRequest("<p>first</p><p>second<br>third</p>"));

        assertThat(validated.content()).isEqualTo("first second third");
    }

    @Test
    void textMessageRejectsOnlyNonBreakingSpaces() {
        assertChatError(textRequest("&nbsp;&nbsp;"), ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void rejectsNonUuidClientMessageId() {
        SendMessageRequest request = new SendMessageRequest("not-a-uuid", MessageType.TEXT, "hello", null, null, null, null, null);

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void rejectsNonCanonicalUuidClientMessageId() {
        SendMessageRequest request = new SendMessageRequest("1-1-1-1-1", MessageType.TEXT, "hello", null, null, null, null, null);

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void rejectsWhitespaceAroundClientMessageId() {
        SendMessageRequest request = new SendMessageRequest(
                " " + UUID.randomUUID() + " ", MessageType.TEXT, "hello", null, null, null, null, null);

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void mediaMessageRejectsInsecureUrlIndependently() {
        SendMessageRequest request = mediaRequest(
                MessageType.IMAGE,
                new MediaMetadataRequest("http://host/a.jpg", "p1", "image/jpeg", 10L, "a.jpg", null, null, null));

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void mediaMessageRejectsMismatchedMimeTypeIndependently() {
        SendMessageRequest request = mediaRequest(
                MessageType.IMAGE,
                new MediaMetadataRequest("https://host/a.jpg", "p1", "video/mp4", 10L, "a.jpg", null, null, null));

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void fileMessageAcceptsNonBlankMimeTypeAndSerializesMetadata() {
        SendMessageRequest request = mediaRequest(
                MessageType.FILE,
                new MediaMetadataRequest("https://host/a.pdf", "p1", "application/pdf", 10L, "a.pdf", null, null, null));

        ChatMessageValidator.ValidatedMessage validated = validator.validate(request);

        assertThat(validated.content()).isNull();
        assertThat(validated.metadata()).isEqualTo(new MediaMetadataRequest("https://host/a.pdf", "p1", "application/pdf", 10L, "a.pdf", null, null, null));
    }

    @Test
    void fileMessageRejectsBlankMimeType() {
        SendMessageRequest request = mediaRequest(
                MessageType.FILE,
                new MediaMetadataRequest("https://host/a.pdf", "p1", " ", 10L, "a.pdf", null, null, null));

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void mediaMessageRejectsNullSize() {
        SendMessageRequest request = mediaRequest(
                MessageType.AUDIO,
                new MediaMetadataRequest("https://host/a.mp3", "p1", "audio/mpeg", null, "a.mp3", null, null, null));

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1L, 0L})
    void mediaMessageRejectsNonPositiveSize(long size) {
        SendMessageRequest request = mediaRequest(
                MessageType.VIDEO,
                new MediaMetadataRequest("https://host/a.mp4", "p1", "video/mp4", size, "a.mp4", null, null, null));

        assertChatError(request, ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @Test
    void textMessageRejectsMediaMetadata() {
        SendMessageRequest request = new SendMessageRequest(
                UUID.randomUUID().toString(),
                MessageType.TEXT,
                "hello",
                new MediaMetadataRequest("https://host/a.jpg", "p1", "image/jpeg", 10L, "a.jpg", null, null, null),
                null,
                null,
                null,
                null);

        assertChatError(request, ErrorCode.CHAT_MESSAGE_TYPE_INVALID);
    }

    @Test
    void imageMessageRejectsTextPayload() {
        SendMessageRequest request = new SendMessageRequest(
                UUID.randomUUID().toString(),
                MessageType.IMAGE,
                "caption",
                new MediaMetadataRequest("https://host/a.jpg", "p1", "image/jpeg", 10L, "a.jpg", null, null, null),
                null,
                null,
                null,
                null);

        assertChatError(request, ErrorCode.CHAT_MESSAGE_TYPE_INVALID);
    }

    @Test
    void storyReplyRequiresTextAndStoryContextAndForbidsMediaMetadata() {
        StoryContextRequest context = storyContext(0L);

        ChatMessageValidator.ValidatedMessage validated = validator.validate(new SendMessageRequest(
                UUID.randomUUID().toString(), MessageType.STORY_REPLY, "<b>hello</b>", null,
                null, null, null, context));

        assertThat(validated.content()).isEqualTo("hello");
        assertThat(validated.storyContext()).isEqualTo(context);

        assertChatError(new SendMessageRequest(
                UUID.randomUUID().toString(), MessageType.STORY_REPLY, " ", null,
                null, null, null, context), ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
        assertChatError(new SendMessageRequest(
                UUID.randomUUID().toString(), MessageType.STORY_REPLY, "hello", null,
                null, null, null, null), ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
        assertChatError(new SendMessageRequest(
                UUID.randomUUID().toString(), MessageType.STORY_REPLY, "hello",
                new MediaMetadataRequest("https://host/a.jpg", "p1", "image/jpeg", 10L, "a.jpg", null, null, null),
                null, null, null, context), ErrorCode.CHAT_MESSAGE_TYPE_INVALID);
    }

    @Test
    void storyReplyRejectsNegativePreviewTime() {
        assertChatError(new SendMessageRequest(
                UUID.randomUUID().toString(), MessageType.STORY_REPLY, "hello", null,
                null, null, null, storyContext(-1L)), ErrorCode.CHAT_MESSAGE_CONTENT_INVALID);
    }

    @ParameterizedTest
    @CsvSource({
            "IMAGE,image/jpeg",
            "VIDEO,video/mp4",
            "AUDIO,audio/mpeg"
    })
    void mediaMessageAcceptsMatchingMimePrefix(MessageType messageType, String mimeType) {
        SendMessageRequest request = mediaRequest(
                messageType,
                new MediaMetadataRequest("https://host/media", "p1", mimeType, 10L, "media", null, null, null));

        assertThat(validator.validate(request).metadata().mimeType()).isEqualTo(mimeType);
    }

    private SendMessageRequest textRequest(String content) {
        return new SendMessageRequest(UUID.randomUUID().toString(), MessageType.TEXT, content, null, null, null, null, null);
    }

    private SendMessageRequest mediaRequest(MessageType messageType, MediaMetadataRequest metadata) {
        return new SendMessageRequest(UUID.randomUUID().toString(), messageType, null, metadata, 4L, null, null, null);
    }

    private StoryContextRequest storyContext(long previewAtMs) {
        return new StoryContextRequest(
                "story-1", "owner-1", "IMAGE", previewAtMs, Instant.parse("2026-08-01T00:00:00Z"));
    }

    private void assertChatError(SendMessageRequest request, ErrorCode expected) {
        assertThatThrownBy(() -> validator.validate(request))
                .isInstanceOfSatisfying(AppException.class,
                        error -> assertThat(error.getErrorCode()).isEqualTo(expected));
    }

    private static MediaPolicyProperties mediaPolicy() {
        MediaPolicyProperties properties = new MediaPolicyProperties();
        properties.setImage(DataSize.ofMegabytes(100));
        properties.setVideo(DataSize.ofMegabytes(100));
        properties.setAudio(DataSize.ofMegabytes(50));
        return properties;
    }
}
