package com.dauducbach.clone.modules.feed.service;

import com.dauducbach.clone.configuration.PostPopularityProperties;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.feed.dto.FeedSourceCursor;
import com.dauducbach.clone.modules.feed.dto.FeedCursorState;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import org.junit.jupiter.api.Test;
import java.time.Instant;
import static org.junit.jupiter.api.Assertions.*;

class FeedCursorCodecTest {
    private final Instant now = Instant.parse("2026-10-06T00:00:00Z");
    private FeedCursorCodec codec() {
        PostPopularityProperties properties = new PostPopularityProperties();
        properties.setCursorSecret("01234567890123456789012345678901");
        return new FeedCursorCodec(properties);
    }
    @Test void signsAndRoundTripsBothSourceAnchors() {
        var codec = codec();
        var start = codec.start("viewer", MediaDisplayType.FEED, now);
        Instant precise = now.minusSeconds(10).plusNanos(123456);
        var state = new FeedCursorState(start.viewer(), start.media(), start.version(), start.startedAt(), start.expiresAt(),
                new FeedSourceCursor(precise.toEpochMilli(), "friend", precise), new FeedSourceCursor(now.minusSeconds(5).toEpochMilli(), "popular"), false, true);
        var token = codec.encode(state);
        assertEquals(state, codec.decode(token, "viewer", MediaDisplayType.FEED, now));
    }
    @Test void rejectsTamperingAndWrongViewer() {
        var codec = codec();
        String token = codec.encode(codec.start("viewer", MediaDisplayType.FEED, now));
        assertEquals(ErrorCode.FEED_CURSOR_INVALID, assertThrows(AppException.class, () -> codec.decode(token, "other", MediaDisplayType.FEED, now)).getErrorCode());
        assertThrows(AppException.class, () -> codec.decode("x" + token, "viewer", MediaDisplayType.FEED, now));
        assertThrows(AppException.class, () -> codec.decode(token, "viewer", MediaDisplayType.AVATAR, now));
    }
    @Test void expiresAfterTwentyMinutes() {
        var codec = codec();
        String token = codec.encode(codec.start("viewer", MediaDisplayType.FEED, now));
        assertEquals(ErrorCode.FEED_CURSOR_EXPIRED, assertThrows(AppException.class, () -> codec.decode(token, "viewer", MediaDisplayType.FEED, now.plusSeconds(1200))).getErrorCode());
    }
}
