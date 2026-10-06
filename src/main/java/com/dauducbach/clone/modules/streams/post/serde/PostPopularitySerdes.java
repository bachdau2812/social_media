package com.dauducbach.clone.modules.streams.post.serde;

import com.dauducbach.clone.modules.post.dto.event.PostEventJson;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/** A local mapper without Redis polymorphic typing or global mapper mutations. */
public final class PostPopularitySerdes {
    private PostPopularitySerdes() {}

    public static String json(Object value) {
        try { return PostEventJson.json(value); }
        catch (Exception error) { throw new SerializationException("Cannot serialize popularity state", error); }
    }

    public static <T> T read(String value, Class<T> type) {
        try { return PostEventJson.read(value, type); }
        catch (Exception error) { throw new SerializationException("Cannot parse popularity state", error); }
    }

    public static <T> Serde<T> of(Class<T> type) {
        return Serdes.serdeFrom((topic, value) -> value == null ? null : json(value).getBytes(java.nio.charset.StandardCharsets.UTF_8),
                (topic, bytes) -> bytes == null ? null : read(new String(bytes, java.nio.charset.StandardCharsets.UTF_8), type));
    }
}
