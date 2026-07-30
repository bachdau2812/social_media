package com.dauducbach.clone.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.BasicPolymorphicTypeValidator;
import com.fasterxml.jackson.databind.jsontype.PolymorphicTypeValidator;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

@Configuration
public class RedisConfig {

    @Bean
    @SuppressWarnings({"deprecation", "removal"})
    public ReactiveRedisTemplate<String, Object> reactiveRedisTemplate(
            ReactiveRedisConnectionFactory factory
    ) {
        PolymorphicTypeValidator validator = BasicPolymorphicTypeValidator.builder()
                .allowIfBaseType(Object.class)
                .build();
        ObjectMapper objectMapper = JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .activateDefaultTyping(
                        validator,
                        ObjectMapper.DefaultTyping.NON_FINAL,
                        JsonTypeInfo.As.PROPERTY)
                .build();

        org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer valueSerializer =
                new org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer(
                        objectMapper);
        StringRedisSerializer stringSerializer = new StringRedisSerializer();
        RedisSerializationContext<String, Object> context = RedisSerializationContext
                .<String, Object>newSerializationContext(stringSerializer)
                .key(stringSerializer)
                .value(valueSerializer)
                .hashKey(stringSerializer)
                .hashValue(valueSerializer)
                .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }

    @Bean
    public ReactiveStringRedisTemplate reactiveStringRedisTemplate(
            ReactiveRedisConnectionFactory factory
    ) {
        return new ReactiveStringRedisTemplate(factory);
    }

    @Bean
    public ReactiveRedisMessageListenerContainer reactiveRedisMessageListenerContainer(
            ReactiveRedisConnectionFactory factory
    ) {
        return new ReactiveRedisMessageListenerContainer(factory);
    }
}