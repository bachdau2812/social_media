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

    /**
     * Get Gson instance for JSON operations
     */
    public static Gson getGson() {
        return gson;
    }

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

    /**
     * Chuyển một đối tượng Java sang JsonObject sử dụng Gson.
     * Nếu object là null → trả về JsonObject rỗng.
     * Nếu object khi chuyển thành JsonElement không phải JsonObject (ví dụ: primitive hoặc array),
     * thì kết quả sẽ được bọc trong một JsonObject với key "value".
     */
    public static JsonObject fromObject(Object obj) {
        if (obj == null) {
            return new JsonObject();
        }
        try {
            Gson gson = new Gson();
            JsonElement element = gson.toJsonTree(obj);
            if (element == null || element.isJsonNull()) {
                return new JsonObject();
            }
            if (element.isJsonObject()) {
                return element.getAsJsonObject();
            }
            // Nếu không phải JsonObject (ví dụ: primitive, array), đóng gói vào trường `value`
            JsonObject wrapper = new JsonObject();
            wrapper.add("value", element);
            return wrapper;
        } catch (Exception e) {
            log.error("Lỗi khi chuyển object sang JsonObject: {}", obj, e);
            return new JsonObject();
        }
    }
}