package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.dto.request.CreateUserRequest;
import com.dauducbach.clone.modules.auth.dto.request.EmailVerifyRequest;
import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE,  makeFinal = true)
public class UserCredentialsService {
    Logger logger = LoggerFactory.getLogger(UserCredentialsService.class);

    R2dbcEntityTemplate r2dbcEntityTemplate;
    PasswordEncoder passwordEncoder;
    ReactiveRedisTemplate<String, Object> reactiveRedisTemplate;
    KafkaSender<String, Object> kafkaSender;
    UserCredentialsRepository userCredentialsRepository;
    ObjectMapper objectMapper;

    public Mono<Void> preRegister(CreateUserRequest request) {
        logger.info("|UserCredentialsService|preRegister|request={}", request.toString());

        return Mono.zip(userCredentialsRepository.existsByUsername(request.getUsername()),
                        userCredentialsRepository.existsByEmail(request.getEmail()))
                .flatMap(tuple -> {
                    boolean existsUsername = tuple.getT1();
                    boolean existsEmail = tuple.getT2();

                    if (existsUsername) {
                        return Mono.error(new AppException(ErrorCode.USERNAME_EXISTS));
                    }

                    if (existsEmail) {
                        return Mono.error(new RuntimeException("Email already exists"));
                    }

                    // Temporary save user information on Redis with an expiration time
                    String userInformationKey = "user_registration:" + request.getEmail();

                    return reactiveRedisTemplate.opsForValue()
                            .set(userInformationKey, request, Duration.ofMinutes(5))
                            .doOnSuccess(success -> logger.info("|UserCredentialsService|preRegister|userInformationKey={}", userInformationKey))
                            .then(sendCode(request.getEmail(), request.getUsername()))
                            .doOnSuccess(sendCodeSuccess -> logger.info("|UserCredentialsService|preRegister|sendCodeToUser=true"))
                            .then();
                });
    }

    public Mono<String> emailVerifyAndCreateUser(EmailVerifyRequest request) {
        logger.info("|UserCredentialsService|emailVerifyAndCreateUser|request={}", request.toString());

        String userInformationKey = "user_registration:" + request.getEmail();
        String emailVerifyForUserKey = "registration_verify:" + request.getEmail();

        return Mono.zip(reactiveRedisTemplate.opsForValue().get(userInformationKey),
                reactiveRedisTemplate.opsForValue().get(emailVerifyForUserKey)
        ).flatMap(objects -> {
            var userRequest = objectMapper.convertValue(objects.getT1(), CreateUserRequest.class);
            // Check is registration information expired or not
            if (userRequest == null) {
                return Mono.error(new AppException(ErrorCode.INVALID_REGISTRATION_REQUEST_INFO));
            }

            // Check is code expired or not
            var code = String.valueOf(objects.getT2());
            if (code == null) {
                return Mono.error(new AppException(ErrorCode.INVALID_REGISTRATION_CODE_INFO));
            }

            // Check valid code for user
            boolean isValid = code.equals(request.getCode());
            if (!isValid) {
                logger.info("|UserCredentialsService|emailVerifyAndCreateUser|validCode=false(expectedCode={} | actualCode={})", code, request.getCode());
                return Mono.error(new AppException(ErrorCode.INVALID_VERIFICATION_CODE));
            }
            logger.info("|UserCredentialsService|emailVerifyAndCreateUser|validCode=true");

            return r2dbcEntityTemplate.insert(UserCredentials.class).using(UserCredentials.builder()
                            .userId(UUID.randomUUID().toString())
                            .username(userRequest.getUsername())
                            .email(userRequest.getEmail())
                            .userPassword(passwordEncoder.encode(userRequest.getPassword()))
                            .userRole(userRequest.getRole())
                            .build())
                    .doOnError(throwable -> logger.error("|UserCredentialsService|emailVerifyAndCreateUser|insert_error={}", throwable.getMessage()))
                    .doOnSuccess(createdUser -> logger.info("|UserCredentialsService|emailVerifyAndCreateUser|insert_success={}", createdUser.getUserId()))
                    .then(Mono.defer(() -> {
                        // Create user and send events to create profile and send notification email
                        var user = UserCredentials.builder()
                                .userId(UUID.randomUUID().toString())
                                .username(userRequest.getUsername())
                                .email(userRequest.getEmail())
                                .userPassword(passwordEncoder.encode(userRequest.getPassword()))
                                .provider("SYSTEM")
                                .build();
                        /// The notification and user modules subscribe to this event and handle their own logic for user creation.
                        JsonObject eventPayload = GsonUtils.fromObject(userRequest);
                        eventPayload.addProperty("userId", user.getUserId());

                        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("profile_creation_event", user.getUserId(), eventPayload.toString());
                        SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "User Creation Event");

                        return kafkaSender.send(Mono.just(senderRecord))
                                .doOnComplete(() -> logger.info("|UserCredentialsService|emailVerifyAndCreateUser|send profile_creation_event complete"))
                                .onErrorResume(error -> {
                                    logger.error("|UserCredentialsService|emailVerifyAndCreateUser|Error sending profile_creation_event: {}", error.getMessage());

                                    return Mono.error(new AppException(ErrorCode.KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL));
                                })
                                .then(Mono.just("Register success"));
                    }));
        });
    }

    @Transactional
    /// Forget password
    public Mono<String> checkAndSendCodeForForgetPassword(String email) {
        logger.info("|UserCredentialsService|checkAndSendCodeForForgetPassword|email={}", email);
        return userCredentialsRepository.existsByEmail(email)
                .flatMap(exists -> {
                    if (!exists) {
                        logger.info("|UserCredentialsService|checkAndSendCodeForForgetPassword|emailNotLinked=true");
                        return Mono.error(new AppException(ErrorCode.EMAIL_NOT_LINKED));
                    }

                    // Send code to Notification Service to send email to user
                    String code = RandomCode.generateRandomCode(10);

                    JsonObject forgetPasswordEvent = new JsonObject();
                    forgetPasswordEvent.addProperty("code", code);
                    forgetPasswordEvent.addProperty("email", email);

                    ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("forget_password_event", email, forgetPasswordEvent.toString());
                    SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "Gui code de nguoi dung xac nhan email de doi mat khau");
                    return kafkaSender.send(Mono.just(senderRecord))
                            .doOnComplete(() -> logger.info("|UserCredentialsService|checkAndSendCodeForForgetPassword|send forget_password_event complete"))
                            .doOnError(throwable -> logger.info("|UserCredentialsService|checkAndSendCodeForForgetPassword|send forget_password_event error={}", throwable.getMessage()))
                            .onErrorMap(throwable ->  new AppException(ErrorCode.KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL))
                            .then(reactiveRedisTemplate.opsForValue().set("forget_password:" + email, code, Duration.ofMinutes(2)));

                })
                .thenReturn("Check email and send code success");
    }

    @Transactional
    public Mono<String> verifyAndSendNewPasswordToUser(EmailVerifyRequest request) {
        return reactiveRedisTemplate.opsForValue().get("forget_password:" + request.getEmail())
                .doOnSuccess(code -> logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|getCodeFromRedis={}", code))
                .flatMap(code -> {
                    if (code == null) {
                        logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|codeExpired=true");
                        return Mono.error(new AppException(ErrorCode.TIMEOUT));
                    }
                    logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|codeExpired=false({})", code);

                    if (!code.equals(request.getCode())) {
                        logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|validCode=false(expectedCode={} | actualCode={})", code, request.getCode());
                        return Mono.error(new AppException(ErrorCode.INVALID_VERIFICATION_CODE));
                    }

                    String newPassword = RandomCode.generateRandomPassword(12);

                    return userCredentialsRepository.findByEmail(request.getEmail())
                            .flatMap(user -> {
                                user.setUserPassword(passwordEncoder.encode(newPassword));

                                /// Send new password to Notification Service to send email to user
                                JsonObject newPasswordEvent = new JsonObject();
                                newPasswordEvent.addProperty("email", request.getEmail());
                                newPasswordEvent.addProperty("newPassword", newPassword);

                                ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("new_password_event", request.getEmail(), newPasswordEvent.toString());
                                SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "Send new password to User");

                                return kafkaSender.send(Mono.just(senderRecord))
                                        .doOnComplete(() -> logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|send new_password_event complete"))
                                        .onErrorMap(throwable -> {
                                            logger.error("|UserCredentialsService|verifyAndSendNewPasswordToUser|Error sending new_password_event: {}", throwable.getMessage());
                                            return new AppException(ErrorCode.KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL);
                                        })
                                        .then(userCredentialsRepository.save(user))
                                        .doOnSuccess(user1 -> logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|updateNewPasswordSuccess|userId={}", user1.getUserId()))
                                        .onErrorMap(throwable -> {
                                            logger.error("|UserCredentialsService|verifyAndSendNewPasswordToUser|updateNewPasswordFail: {}", throwable.getMessage());
                                            return new AppException(ErrorCode.KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL);
                                        })
                                        .then(Mono.just("New password was set and sent to your email successfully"));
                            });
                });
    }

    @Transactional
    public Mono<String> verifyAndSendNewUserNameAndNewPasswordToUser(EmailVerifyRequest request) {
        return reactiveRedisTemplate.opsForValue().get("forget_password:" + request.getEmail())
                .flatMap(code -> {
                    if (code == null) {
                        logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|codeExpired=true");
                        return Mono.error(new AppException(ErrorCode.TIMEOUT));
                    }
                    logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|codeExpired=false({})", code);

                    if (!code.equals(request.getCode())) {
                        logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|validCode=false(expectedCode={} | actualCode={})", code, request.getCode());
                        return Mono.error(new AppException(ErrorCode.INVALID_VERIFICATION_CODE));
                    }

                    String newPassword = RandomCode.generateRandomPassword(12);

                    return userCredentialsRepository.findByEmail(request.getEmail())
                            .flatMap(user -> {
                                user.setUsername(request.getEmail());
                                user.setUserPassword(passwordEncoder.encode(newPassword));

                                /// Send new password to Notification Service to send email to user
                                JsonObject newPasswordEvent = new JsonObject();
                                newPasswordEvent.addProperty("email", request.getEmail());
                                newPasswordEvent.addProperty("newPassword", newPassword);

                                ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("new_password_and_username_event", request.getEmail(), newPasswordEvent.toString());
                                SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "Send new password to User");

                                return kafkaSender.send(Mono.just(senderRecord))
                                        .doOnComplete(() -> logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|send new_password_event complete"))
                                        .onErrorMap(throwable -> {
                                            logger.error("|UserCredentialsService|verifyAndSendNewPasswordToUser|Error sending new_password_event: {}", throwable.getMessage());
                                            return new AppException(ErrorCode.KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL);
                                        })
                                        .then(userCredentialsRepository.save(user))
                                        .doOnSuccess(user1 -> logger.info("|UserCredentialsService|verifyAndSendNewPasswordToUser|updateNewPasswordSuccess|userId={}", user1.getUserId()))
                                        .onErrorMap(throwable -> {
                                            logger.error("|UserCredentialsService|verifyAndSendNewPasswordToUser|updateNewPasswordFail: {}", throwable.getMessage());
                                            return new AppException(ErrorCode.KAFKA_SEND_MESSAGE_FOR_EVENT_FAIL);
                                        })
                                        .then(Mono.just("New password was set and sent to your email successfully"));
                            });
                });
    }

    /// Utils
    private Mono<Void> sendCode(String email, String username) {
        String code = RandomCode.generateRandomCode(8);
        String emailVerifyKey = "registration_verify:" + email;
        return  reactiveRedisTemplate.opsForValue().set(emailVerifyKey, code, Duration.ofMinutes(5))
                .flatMap(saveComplete -> {
                    if (saveComplete) {
                        JsonObject emailVerifyEvent = new JsonObject();

                        emailVerifyEvent.addProperty("username", username);
                        emailVerifyEvent.addProperty("email", email);
                        emailVerifyEvent.addProperty("code", code);

                        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>("auth_send_code", email, emailVerifyEvent.toString());
                        SenderRecord<String, Object, String> senderRecord = SenderRecord.create(producerRecord, "Send verify code for new registration user");
                        return kafkaSender.send(Mono.just(senderRecord))
                                .then();
                    }

                    return Mono.error(new AppException(ErrorCode.CODE_CREATION_FAILED));
                });

    }
}
