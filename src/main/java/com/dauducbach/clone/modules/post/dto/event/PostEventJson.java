package com.dauducbach.clone.modules.post.dto.event;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** Domain JSON contracts, independent of Kafka Streams and Redis polymorphic serialization. */
public final class PostEventJson {
    private static final ObjectMapper JSON = new ObjectMapper().registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private PostEventJson() {}

    public static String json(Object value) {
        try { return JSON.writeValueAsString(value); }
        catch (Exception error) { throw new IllegalArgumentException("Cannot serialize post event", error); }
    }

    public static <T> T read(String value, Class<T> type) {
        try { return JSON.readValue(value, type); }
        catch (Exception error) { throw new IllegalArgumentException("Cannot parse post event", error); }
    }
}
