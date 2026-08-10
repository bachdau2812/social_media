package com.dauducbach.clone.modules.chat.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.chat.constant.MessageType;
import com.dauducbach.clone.modules.chat.dto.request.MediaMetadataRequest;
import com.dauducbach.clone.modules.chat.dto.request.SendMessageRequest;
import com.dauducbach.clone.modules.chat.dto.request.StoryContextRequest;
import com.dauducbach.clone.modules.media.configuration.MediaPolicyProperties;
import lombok.RequiredArgsConstructor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Locale;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class ChatMessageValidator {

    static final long MAX_AUDIO_DURATION_MS = 5L * 60 * 1000;
    private static final String BLOCK_SELECTOR = "address, article, aside, blockquote, div, dl, fieldset, figcaption, figure, footer, form, h1, h2, h3, h4, h5, h6, header, hr, li, main, nav, ol, p, pre, section, table, tr, ul";

    private final MediaPolicyProperties mediaPolicy;

    public ValidatedMessage validate(SendMessageRequest request) {
        if (request == null) {
            throw new AppException(ErrorCode.CHAT_REQUEST_INVALID, "Chat message request is required");
        }
        validateClientMessageId(request.clientMessageId());
        if (request.messageType() == null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_TYPE_INVALID, "Chat message type is required");
        }
        if (request.messageType() == MessageType.SYSTEM) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_TYPE_INVALID, "System messages can only be created by the server");
        }
        if (request.messageType() == MessageType.TEXT) {
            return validateTextMessage(request);
        }
        if (request.messageType() == MessageType.STORY_REPLY) {
            return validateStoryReply(request);
        }
        return validateMediaMessage(request);
    }

    private ValidatedMessage validateTextMessage(SendMessageRequest request) {
        if (request.metadata() != null || request.storyContext() != null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_TYPE_INVALID, "Text messages cannot include media metadata");
        }
        if (request.content() == null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Text message content is required");
        }

        String sanitized = toPlainText(request.content());
        if (sanitized.isBlank()) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Text message content is empty after sanitization");
        }
        return new ValidatedMessage(sanitized, null, null);
    }

    private ValidatedMessage validateStoryReply(SendMessageRequest request) {
        if (request.metadata() != null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_TYPE_INVALID, "Story replies cannot include media metadata");
        }
        if (request.content() == null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Story reply content is required");
        }
        String sanitized = toPlainText(request.content());
        StoryContextRequest context = request.storyContext();
        if (sanitized.isBlank()
                || context == null
                || isBlank(context.storyId())
                || isBlank(context.storyOwnerId())
                || !("IMAGE".equalsIgnoreCase(context.mediaType()) || "VIDEO".equalsIgnoreCase(context.mediaType()))
                || context.previewAtMs() == null
                || context.previewAtMs() < 0
                || context.expiresAt() == null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Story reply is invalid");
        }
        StoryContextRequest normalized = new StoryContextRequest(
                context.storyId().trim(),
                context.storyOwnerId().trim(),
                context.mediaType().trim().toUpperCase(Locale.ROOT),
                context.previewAtMs(),
                context.expiresAt());
        return new ValidatedMessage(sanitized, null, normalized);
    }

    private ValidatedMessage validateMediaMessage(SendMessageRequest request) {
        if (request.storyContext() != null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_TYPE_INVALID, "Media messages cannot include Story context");
        }
        if (request.content() != null && !request.content().isBlank()) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_TYPE_INVALID, "Media messages cannot include text content");
        }
        MediaMetadataRequest metadata = request.metadata();
        if (metadata == null) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Media metadata is required");
        }

        validateMetadata(metadata, request.messageType());
        return new ValidatedMessage(null, metadata, null);
    }

    private void validateClientMessageId(String clientMessageId) {
        if (clientMessageId == null || !clientMessageId.equals(clientMessageId.trim())) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "clientMessageId must be an untrimmed canonical UUID");
        }
        try {
            UUID parsed = UUID.fromString(clientMessageId);
            if (!parsed.toString().equalsIgnoreCase(clientMessageId)) {
                throw new IllegalArgumentException("UUID is not canonical");
            }
        } catch (IllegalArgumentException error) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "clientMessageId must be a canonical UUID", error);
        }
    }

    private void validateMetadata(MediaMetadataRequest metadata, MessageType messageType) {
        if (!isHttpsUrl(metadata.url())
                || isBlank(metadata.publicId())
                || isBlank(metadata.mimeType())
                || metadata.size() == null
                || metadata.size() <= 0
                || isBlank(metadata.fileName())) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Media metadata is invalid");
        }

        String mimeType = metadata.mimeType().toLowerCase(Locale.ROOT);
        boolean matchesType = switch (messageType) {
            case IMAGE -> mimeType.startsWith("image/");
            case VIDEO -> mimeType.startsWith("video/");
            case AUDIO -> mimeType.startsWith("audio/") || mimeType.equals("video/webm");
            case FILE -> true;
            case TEXT, STORY_REPLY, SYSTEM -> false;
        };
        if (!matchesType) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Media MIME type does not match message type");
        }

        long maximumBytes = switch (messageType) {
            case IMAGE -> mediaPolicy.imageMaxBytes();
            case VIDEO -> mediaPolicy.videoMaxBytes();
            case AUDIO, FILE -> mediaPolicy.audioMaxBytes();
            case TEXT, STORY_REPLY, SYSTEM -> 0;
        };
        if (metadata.size() > maximumBytes) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Chat media exceeds the allowed size");
        }
        if (metadata.width() != null && metadata.width() < 0
                || metadata.height() != null && metadata.height() < 0
                || metadata.duration() != null && metadata.duration() < 0) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Media dimensions or duration are invalid");
        }
        if (messageType == MessageType.AUDIO
                && metadata.duration() != null
                && metadata.duration() > MAX_AUDIO_DURATION_MS) {
            throw new AppException(ErrorCode.CHAT_MESSAGE_CONTENT_INVALID, "Voice message exceeds five minutes");
        }
    }

    private String toPlainText(String content) {
        Document document = Jsoup.parseBodyFragment(content);
        document.select("script, style").remove();
        document.select("br").after(" ");
        document.select(BLOCK_SELECTOR).after(" ");
        return document.body().text().replace('\u00A0', ' ').trim();
    }

    private boolean isHttpsUrl(String value) {
        if (isBlank(value)) {
            return false;
        }
        try {
            URI uri = new URI(value);
            return "https".equalsIgnoreCase(uri.getScheme()) && !isBlank(uri.getHost());
        } catch (URISyntaxException error) {
            return false;
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    public record ValidatedMessage(
            String content,
            MediaMetadataRequest metadata,
            StoryContextRequest storyContext) {
    }
}
