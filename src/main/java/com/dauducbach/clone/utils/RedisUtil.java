package com.dauducbach.clone.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Component
public class RedisUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    /**
     * Serialize object to JSON String for storage in Redis
     */
    public static String serialize(Object object) {
        if (object == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            log.error("Error serializing object to JSON: {}", object, e);
            return null;
        }
    }

    /**
     * Deserialize JSON String from Redis to specified class type
     */
    public static <T> T deserialize(String jsonString, Class<T> clazz) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return null;
        }
        try {
            return objectMapper.readValue(jsonString, clazz);
        } catch (Exception e) {
            log.error("Error deserializing JSON to object: {}", jsonString, e);
            return null;
        }
    }

    /**
     * Deserialize JSON String list from Redis to List of specified class type
     */
    public static <T> List<T> deserializeList(String jsonString, Class<T> clazz) {
        if (jsonString == null || jsonString.trim().isEmpty()) {
            return new ArrayList<>();
        }
        try {
            TypeFactory typeFactory = objectMapper.getTypeFactory();
            return objectMapper.readValue(jsonString, typeFactory.constructCollectionType(List.class, clazz));
        } catch (Exception e) {
            log.error("Error deserializing JSON to list: {}", jsonString, e);
            return new ArrayList<>();
        }
    }

    /**
     * Convert object to specified class type (useful when working with reactive tuples)
     */
    public static <T> T convertValue(Object object, Class<T> clazz) {
        if (object == null) {
            return null;
        }
        try {
            return objectMapper.convertValue(object, clazz);
        } catch (Exception e) {
            log.error("Error converting object to class: {}", object, e);
            return null;
        }
    }
}
