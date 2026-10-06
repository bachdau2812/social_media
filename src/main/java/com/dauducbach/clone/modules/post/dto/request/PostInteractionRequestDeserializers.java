package com.dauducbach.clone.modules.post.dto.request;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;

import java.io.IOException;

/** Exact token rules for this ingestion request, independent of global Jackson coercion. */
public final class PostInteractionRequestDeserializers {
    private PostInteractionRequestDeserializers() {}

    public static final class StrictBoolean extends JsonDeserializer<Boolean> {
        @Override
        public Boolean deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (parser.hasToken(JsonToken.VALUE_TRUE)) {
                return true;
            }
            if (parser.hasToken(JsonToken.VALUE_FALSE)) {
                return false;
            }
            throw JsonMappingException.from(parser, "isClick must be a JSON boolean");
        }
    }

    public static final class StrictViewTime extends JsonDeserializer<Integer> {
        @Override
        public Integer deserialize(JsonParser parser, DeserializationContext context) throws IOException {
            if (!parser.hasToken(JsonToken.VALUE_NUMBER_INT)) {
                throw JsonMappingException.from(parser, "viewTime must be a JSON integer from 0 to 3600");
            }
            int seconds = parser.getIntValue();
            if (seconds < 0 || seconds > 3600) {
                throw JsonMappingException.from(parser, "viewTime must be from 0 to 3600 seconds");
            }
            return seconds;
        }
    }
}
