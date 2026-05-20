package com.dauducbach.clone.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GsonUtils {

    /**
     * Chuyển từ String (nhận từ Kafka) sang JsonObject
     */
    public static JsonObject fromString(String jsonString) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return new JsonObject();
        }
        try {
            // Parse chuỗi thành JsonObject một cách an toàn
            return JsonParser.parseString(jsonString).getAsJsonObject();
        } catch (JsonSyntaxException | IllegalStateException e) {
            log.error("Lỗi khi parse chuỗi JSON từ Kafka: {}", jsonString, e);
            // Trả về một object rỗng thay vì throw Exception để luồng Kafka không bị chết (crash)
            return new JsonObject(); 
        }
    }

    /**
     * Chuyển từ JsonObject sang String (để gửi lên Kafka)
     */
    public static String toString(JsonObject jsonObject) {
        return jsonObject != null ? jsonObject.toString() : "{}";
    }
}