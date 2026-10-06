package com.dauducbach.clone.modules.post.dto.request;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PostInteractionRequestTest {
    private final ObjectMapper mapper = new ObjectMapper();

    private String json(String click, String dwell) {
        return "{\"postId\":\"post\",\"isClick\":" + click + ",\"viewTime\":" + dwell
                + ",\"eventId\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\","
                + "\"impressionId\":\"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\",\"actor\":\"spoof\",\"score\":99}";
    }

    @Test void acceptsOnlyJsonBooleanAndIntegerSecondsAndIgnoresUntrustedExtras() throws Exception {
        var request = mapper.readValue(json("true", "3600"), PostInteractionRequest.class);
        assertTrue(request.isClick());
        assertEquals(3600, request.viewTime());
        assertEquals("post", request.postId());
        assertFalse(mapper.readValue(json("false", "0"), PostInteractionRequest.class).isClick());
    }

    @Test void rejectsFractionalOutOfRangeAndStringSeconds() {
        for (String dwell : new String[]{"3600.9", "-0.5", "1.0", "1e2", "-1", "3601", "2147483648", "\"30\""}) {
            assertThrows(JsonProcessingException.class,
                    () -> mapper.readValue(json("true", dwell), PostInteractionRequest.class), dwell);
        }
    }

    @Test void rejectsNumericAndStringClick() {
        for (String click : new String[]{"1", "0", "\"true\"", "\"false\""}) {
            assertThrows(JsonProcessingException.class,
                    () -> mapper.readValue(json(click, "30"), PostInteractionRequest.class), click);
        }
    }
}
