package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.modules.feed.dto.FeedCursorState;
import com.dauducbach.clone.modules.feed.dto.FeedSourceCursor;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Objects;

@Component
public class FeedCursorCodec {
    public static final String VERSION = "feed-popular-v1";
    private final PostPopularityProperties properties;
    private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
    private static final Base64.Encoder ENCODER = Base64.getUrlEncoder().withoutPadding();
    private static final Base64.Decoder DECODER = Base64.getUrlDecoder();

    public FeedCursorCodec(PostPopularityProperties properties) { this.properties = properties; }
    public FeedCursorState start(String viewer, MediaDisplayType media, Instant now) {
        return new FeedCursorState(viewer, media.name(), VERSION, now.toEpochMilli(),
                now.plus(properties.getCursorLifetime()).toEpochMilli(), null, null, false, false);
    }
    public String encode(FeedCursorState state) {
        try {
            String payload = ENCODER.encodeToString(mapper.writeValueAsBytes(state));
            return payload + "." + ENCODER.encodeToString(sign(payload));
        } catch (Exception error) { throw new IllegalStateException("Cannot sign feed cursor", error); }
    }
    public FeedCursorState decode(String token, String viewer, MediaDisplayType media, Instant now) {
        try {
            if (token == null || token.length() > 8192) throw invalid();
            String[] parts = token.split("\\.", -1);
            if (parts.length != 2 || !MessageDigest.isEqual(sign(parts[0]), DECODER.decode(parts[1]))) throw invalid();
            FeedCursorState state = mapper.readValue(DECODER.decode(parts[0]), FeedCursorState.class);
            if (!Objects.equals(viewer, state.viewer()) || !Objects.equals(media.name(), state.media())
                    || !VERSION.equals(state.version()) || state.startedAt() > now.toEpochMilli()
                    || state.expiresAt() <= state.startedAt() || !validAnchor(state.friends(), state.startedAt())
                    || !validAnchor(state.popular(), state.startedAt())) throw invalid();
            if (state.expiresAt() <= now.toEpochMilli()) throw new AppException(ErrorCode.FEED_CURSOR_EXPIRED);
            return state;
        } catch (AppException error) { throw error;
        } catch (Exception error) { throw invalid(); }
    }
    private boolean validAnchor(FeedSourceCursor anchor, long upperBound) {
        return anchor == null || (anchor.postId() != null && !anchor.postId().isBlank()
                && anchor.timeMs() <= upperBound
                && (anchor.preciseTime() == null || anchor.preciseTime().toEpochMilli() == anchor.timeMs()));
    }
    private byte[] sign(String payload) throws Exception {
        String secret = properties.getCursorSecret();
        if (secret == null || secret.length() < 32) throw new IllegalStateException("Feed cursor secret requires 32 characters");
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        return mac.doFinal(payload.getBytes(StandardCharsets.US_ASCII));
    }
    private AppException invalid() { return new AppException(ErrorCode.FEED_CURSOR_INVALID); }
}
