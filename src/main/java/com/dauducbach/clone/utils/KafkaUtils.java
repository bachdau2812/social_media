package com.dauducbach.clone.utils;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class cho các thao tác xử lý Kafka messages
 * Hỗ trợ parse ngày tháng, extract fields, và các operations phổ biến
 */
@Slf4j
@Component
public class KafkaUtils {

    // Các format ngày tháng có thể nhận từ Kafka
    private static final DateTimeFormatter[] DATE_FORMATTERS = {
            DateTimeFormatter.ofPattern("MMM dd, yyyy, h:mm:ss a"), // Jan 15, 1998, 7:00:00 AM
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),             // 1998-01-15
            DateTimeFormatter.ofPattern("MM/dd/yyyy"),              // 01/15/1998
            DateTimeFormatter.ofPattern("dd/MM/yyyy"),              // 15/01/1998
            DateTimeFormatter.ISO_LOCAL_DATE,                      // ISO format
            DateTimeFormatter.ofPattern("MMM dd, yyyy")             // Jan 15, 1998
    };

    // Default value cho các parse failures
    private static final LocalDate DEFAULT_DATE = LocalDate.of(1998, 1, 1);
    private static final String DEFAULT_STRING = "";
    private static final Integer DEFAULT_INTEGER = 0;
    private static final Boolean DEFAULT_BOOLEAN = false;

    /**
     * Parse ngày tháng từ string với nhiều format khác nhau
     * @param dateString Chuỗi ngày cần parse
     * @return LocalDate đã parse, hoặc DEFAULT_DATE nếu fail
     */
    public static LocalDate parseDate(String dateString) {
        if (dateString == null || dateString.trim().isEmpty()) {
            return DEFAULT_DATE;
        }

        // Xóa quotes và whitespace nếu có
        String cleanDate = cleanString(dateString);

        // Thử từng format
        for (DateTimeFormatter formatter : DATE_FORMATTERS) {
            try {
                LocalDate parsed = LocalDate.parse(cleanDate, formatter);
                log.debug("|KafkaUtils|parseDate|parsed successfully: {} -> {}", dateString, parsed);
                return parsed;
            } catch (DateTimeParseException e) {
                // Thử format tiếp theo
            }
        }

        // Nếu tất cả đều fail, log warning và return default value
        log.warn("|KafkaUtils|parseDate|failed to parse date: {}, using default: {}", dateString, DEFAULT_DATE);
        return DEFAULT_DATE;
    }

    /**
     * Xóa quotes và whitespace thừa từ string
     * @param input String cần clean
     * @return String đã clean
     */
    public static String cleanString(String input) {
        if (input == null) {
            return DEFAULT_STRING;
        }
        return input.trim().replaceAll("^\"|\"$", "");
    }

    /**
     * Extract string value từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @return String value, hoặc DEFAULT_STRING nếu không tìm thấy
     */
    public static String extractString(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return DEFAULT_STRING;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                String value = element.getAsString();
                return cleanString(value);
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractString|failed to extract field: {} from jsonObject", fieldName, e);
        }

        return DEFAULT_STRING;
    }

    /**
     * Extract Integer value từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @return Integer value, hoặc DEFAULT_INTEGER nếu không tìm thấy
     */
    public static Integer extractInteger(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return DEFAULT_INTEGER;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                return element.getAsInt();
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractInteger|failed to extract field: {} from jsonObject", fieldName, e);
        }

        return DEFAULT_INTEGER;
    }

    /**
     * Extract Boolean value từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @return Boolean value, hoặc DEFAULT_BOOLEAN nếu không tìm thấy
     */
    public static Boolean extractBoolean(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return DEFAULT_BOOLEAN;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                return element.getAsBoolean();
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractBoolean|failed to extract field: {} from jsonObject", fieldName, e);
        }

        return DEFAULT_BOOLEAN;
    }

    /**
     * Extract LocalDate value từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @return LocalDate value, hoặc DEFAULT_DATE nếu không tìm thấy
     */
    public static LocalDate extractLocalDate(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return DEFAULT_DATE;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                String dateString = element.getAsString();
                return parseDate(dateString);
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractLocalDate|failed to extract field: {} from jsonObject", fieldName, e);
        }

        return DEFAULT_DATE;
    }

    /**
     * Log incoming Kafka payload cho debugging
     * @param topic Topic name
     * @param payload Payload content
     */
    public static void logPayload(String topic, String payload) {
        log.info("|KafkaUtils|logPayload|topic={}|payload={}|length={}", topic, payload, payload != null ? payload.length() : 0);
    }

    /**
     * Validate required fields trong JsonObject
     * @param jsonObject JsonObject cần validate
     * @param requiredFields Danh sách các field bắt buộc
     * @return true nếu tất cả fields đều tồn tại, false nếu thiếu field nào
     */
    public static boolean validateRequiredFields(JsonObject jsonObject, String... requiredFields) {
        if (jsonObject == null || requiredFields == null) {
            log.warn("|KafkaUtils|validateRequiredFields|invalid input: jsonObject or requiredFields is null");
            return false;
        }

        for (String field : requiredFields) {
            if (!jsonObject.has(field) || jsonObject.get(field).isJsonNull()) {
                log.warn("|KafkaUtils|validateRequiredFields|missing required field: {}", field);
                return false;
            }
        }

        return true;
    }

    /**
     * Build error message cho Kafka processing failures
     * @param topic Topic name
     * @param action Action being performed
     * @param error Error message
     * @return Formatted error message
     */
    public static String buildErrorMessage(String topic, String action, String error) {
        return String.format("Kafka processing failed | topic=%s | action=%s | error=%s", topic, action, error);
    }

    /**
     * Parse list of strings từ JsonElement
     * @param element JsonElement chứa list
     * @return List of strings, hoặc empty list nếu fail
     */
    public static List<String> extractStringList(JsonElement element) {
        List<String> result = new ArrayList<>();

        if (element == null || element.isJsonNull()) {
            return result;
        }

        try {
            if (element.isJsonArray()) {
                for (JsonElement item : element.getAsJsonArray()) {
                    if (!item.isJsonNull()) {
                        result.add(cleanString(item.getAsString()));
                    }
                }
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractStringList|failed to parse string list", e);
        }

        return result;
    }

    /**
     * Parse list of strings từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field chứa list
     * @return List of strings, hoặc empty list nếu không tìm thấy
     */
    public static List<String> extractStringList(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return new ArrayList<>();
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            return extractStringList(element);
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractStringList|failed to extract field: {} from jsonObject", fieldName, e);
            return new ArrayList<>();
        }
    }

    /**
     * Parse Long value từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @return Long value, hoặc null nếu không tìm thấy
     */
    public static Long extractLong(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return null;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                return element.getAsLong();
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractLong|failed to extract field: {} from jsonObject", fieldName, e);
        }

        return null;
    }

    /**
     * Parse Double value từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @return Double value, hoặc null nếu không tìm thấy
     */
    public static Double extractDouble(JsonObject jsonObject, String fieldName) {
        if (jsonObject == null || fieldName == null) {
            return null;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                return element.getAsDouble();
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractDouble|failed to extract field: {} from jsonObject", fieldName, e);
        }

        return null;
    }

    /**
     * Get value with fallback default
     * @param value Value cần check
     * @param defaultValue Default value nếu null
     * @return value hoặc defaultValue
     */
    public static <T> T getValueOrDefault(T value, T defaultValue) {
        return value != null ? value : defaultValue;
    }

    /**
     * Parse generic object từ JsonObject field
     * @param jsonObject JsonObject chứa field
     * @param fieldName Tên field cần extract
     * @param clazz Class của object cần parse
     * @return Parsed object, hoặc null nếu không tìm thấy
     */
    public static <T> T extractObject(JsonObject jsonObject, String fieldName, Class<T> clazz) {
        if (jsonObject == null || fieldName == null || clazz == null) {
            return null;
        }

        try {
            JsonElement element = jsonObject.get(fieldName);
            if (element != null && !element.isJsonNull()) {
                return GsonUtils.getGson().fromJson(element, clazz);
            }
        } catch (Exception e) {
            log.warn("|KafkaUtils|extractObject|failed to extract field: {} as class: {}", fieldName, clazz.getSimpleName(), e);
        }

        return null;
    }
}