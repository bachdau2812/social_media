package com.dauducbach.clone.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GsonUtils {
    private static final Gson gson = new Gson();

    public static Gson getGson() {
        return gson;
    }

    public static JsonObject fromString(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return new JsonObject();
        }

        try {
            JsonElement element = JsonParser.parseString(jsonString);
            if (element.isJsonObject()) {
                return element.getAsJsonObject();
            }

            if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
                JsonElement unwrappedElement = JsonParser.parseString(element.getAsString());
                if (unwrappedElement.isJsonObject()) {
                    return unwrappedElement.getAsJsonObject();
                }
            }

            return new JsonObject();
        } catch (JsonSyntaxException | IllegalStateException e) {
            log.error("|GsonUtils|fromString|failed|payloadLength={}", jsonString.length(), e);
            return new JsonObject();
        }
    }

    public static String toString(JsonObject jsonObject) {
        return jsonObject != null ? jsonObject.toString() : "{}";
    }

    public static JsonObject fromObject(Object obj) {
        if (obj == null) {
            return new JsonObject();
        }

        try {
            JsonElement element = gson.toJsonTree(obj);
            if (element == null || element.isJsonNull()) {
                return new JsonObject();
            }
            if (element.isJsonObject()) {
                return element.getAsJsonObject();
            }

            JsonObject wrapper = new JsonObject();
            wrapper.add("value", element);
            return wrapper;
        } catch (Exception e) {
            log.error("|GsonUtils|fromObject|failed|type={}", obj.getClass().getName(), e);
            return new JsonObject();
        }
    }
}
