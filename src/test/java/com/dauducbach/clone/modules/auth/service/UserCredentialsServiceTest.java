package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.dto.request.CreateUserRequest;
import com.dauducbach.clone.modules.auth.dto.request.EmailVerifyRequest;
import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveInsertOperation;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.security.crypto.password.PasswordEncoder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserCredentialsServiceTest {
    @Mock
    R2dbcEntityTemplate r2dbcEntityTemplate;
    @Mock
    PasswordEncoder passwordEncoder;
    @Mock
    ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    @Mock
    ReactiveValueOperations<String, Object> valueOperations;
    @Mock
    KafkaSender<String, String> kafkaSender;
    @Mock
    UserCredentialsRepository userCredentialsRepository;
    @Mock
    ReactiveInsertOperation.ReactiveInsert<UserCredentials> insertSpec;

    @Test
    void emailVerifyAndCreateUserPublishesProfileEventWithInsertedUserId() {
        UserCredentialsService service = newService();
        CreateUserRequest userRequest = CreateUserRequest.builder()
                .username("bach")
                .password("password")
                .email("bach@example.com")
                .role("USER")
                .build();
        UserCredentials createdUser = UserCredentials.builder()
                .userId("credential-user-id")
                .username("bach")
                .email("bach@example.com")
                .userPassword("encoded")
                .userRole("USER")
                .build();

        when(reactiveRedisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("user_registration:bach@example.com")).thenReturn(Mono.just(userRequest));
        when(valueOperations.get("registration_verify:bach@example.com")).thenReturn(Mono.just("123456"));
        when(passwordEncoder.encode("password")).thenReturn("encoded");
        when(r2dbcEntityTemplate.insert(UserCredentials.class)).thenReturn(insertSpec);
        when(insertSpec.using(any(UserCredentials.class))).thenReturn(Mono.just(createdUser));
        when(kafkaSender.send(any(Publisher.class))).thenAnswer(invocation -> {
            Publisher<SenderRecord<String, String, String>> publisher = invocation.getArgument(0);
            StepVerifier.create(Flux.from(publisher))
                    .assertNext(record -> assertProfileCreationRecord(record, "credential-user-id"))
                    .verifyComplete();
            return Flux.empty();
        });

        StepVerifier.create(service.emailVerifyAndCreateUser(EmailVerifyRequest.builder()
                        .email("bach@example.com")
                        .code("123456")
                        .build()))
                .expectNext("Register success")
                .verifyComplete();
    }

    private void assertProfileCreationRecord(SenderRecord<String, String, String> record, String expectedUserId) {
        assertThat(record.topic()).isEqualTo("profile_creation_event");
        assertThat(record.key()).isEqualTo(expectedUserId);

        ProducerRecord<String, String> producerRecord = record;
        JsonObject payload = GsonUtils.fromString(producerRecord.value());
        assertThat(payload.get("userId").getAsString()).isEqualTo(expectedUserId);
        assertThat(payload.get("username").getAsString()).isEqualTo("bach");
    }

    private UserCredentialsService newService() {
        return new UserCredentialsService(
                r2dbcEntityTemplate,
                passwordEncoder,
                reactiveRedisTemplate,
                kafkaSender,
                userCredentialsRepository,
                new ObjectMapper()
        );
    }
}
