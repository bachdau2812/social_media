package com.dauducbach.clone.modules.auth.infrastructure.oauth;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.userinfo.DefaultReactiveOAuth2UserService;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.client.userinfo.ReactiveOAuth2UserService;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)

public class SocialLoginService extends DefaultReactiveOAuth2UserService {
    private static AtomicInteger count = new AtomicInteger(0);
    private static final Logger logger = LoggerFactory.getLogger(SocialLoginService.class);

    R2dbcEntityTemplate r2dbcEntityTemplate;
    UserCredentialsRepository userCredentialsRepository;
    KafkaSender<String, String> kafkaSender;
    PasswordEncoder passwordEncoder;
    WebClient webClient;
    OAuthProviderProfileMapper profileMapper;

    @Override
    public Mono<OAuth2User> loadUser(OAuth2UserRequest userRequest) {
        ReactiveOAuth2UserService<OAuth2UserRequest, OAuth2User> oauth2UserService = new DefaultReactiveOAuth2UserService();

        return oauth2UserService.loadUser(userRequest)
                .doOnSuccess(oAuth2User -> logger.info("|SocialLoginService|providerProfileLoaded"))
                .onErrorMap(throwable -> {
                    logger.warn("|SocialLoginService|providerProfileLoadFailed|errorType={}", throwable.getClass().getSimpleName());
                    return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                })
                .flatMap(oAuth2User -> processUser(userRequest, oAuth2User))
                .doOnSuccess(oAuth2User -> logger.info("|SocialLoginService|loadUser|processingCompleted"));
    }

    public Mono<OAuth2User> processUser(OAuth2UserRequest oAuth2UserRequest, OAuth2User oAuth2User) {
        logger.info("|SocialLoginService|processUser|started");

        /// extract provider common information
        String provider = oAuth2UserRequest.getClientRegistration().getRegistrationId();
        OAuthProviderProfileMapper.ProviderProfile profile = profileMapper.map(oAuth2User, provider);
        String email = profile.email();
        String displayName = profile.displayName();
        logger.info("|SocialLoginService|providerIdentityExtracted|provider={}", provider);

        /// extract provider specific information
        String providerId = profile.providerId();
        String avatarUrl = profile.avatarUrl();

        logger.info("|SocialLoginService|providerProfileMapped|provider={}", provider);

        /// If email is null and provider is GitHub, try to fetch from GitHub API
        Mono<String> emailMono = email != null ? Mono.just(email) : 
            ("github".equals(provider) ? fetchGithubEmail(oAuth2UserRequest) : Mono.just(null));

        return emailMono.flatMap(fetchedEmail -> {
            /// validate required information
            if (fetchedEmail == null || providerId == null) {
                logger.warn("|SocialLoginService|providerProfileIncomplete|provider={}", provider);
                return Mono.error(new AppException(ErrorCode.MISSING_USER_INFO_FROM_SOCIAL_MEDIA));
            }

            return userCredentialsRepository.existsByProviderId(providerId)
                    .flatMap(existed -> {
                        /// if user with providerId already exists, we can directly return the OAuth2User without creating new account
                        if (!existed) {

                            /// check if email already linked to another account
                            return userCredentialsRepository.existsByEmail(fetchedEmail)
                                    .flatMap(emailExisted -> {
                                        /// if email already linked to another account, we should not create new account and return error to client, otherwise we can create new account for this user
                                        if (emailExisted) {
                                            logger.warn("|SocialLoginService|emailAlreadyLinked|provider={}", provider);
                                            return Mono.error(new AppException(ErrorCode.EMAIL_ALREADY_LINKED));
                                        } else {
                                            /// create new account for this user
                                            return createNewUser(fetchedEmail, provider, providerId, avatarUrl, displayName, oAuth2User);
                                        }
                                    });
                        }

                        logger.info("|SocialLoginService|existingProviderAccountFound|provider={}", provider);
                        return Mono.just(oAuth2User);
                    });
        });
    }

    public Mono<OAuth2User> createNewUser(String email, String provider, String providerId, String avatarUrl, String fullName, OAuth2User oAuth2User) {
        UserCredentials newUser = UserCredentials.builder()
                .userId(UUID.randomUUID().toString())
                .username(generateUsername(email))
                .email(email)
                // Default password is user's email
                .userPassword(passwordEncoder.encode(email))
                .userRole("USER")
                .provider(provider)
                .providerId(providerId)
                .build();

        SenderRecord<String, String, String> socialUserCreationRecord = SenderRecord.create(
                new ProducerRecord<>("user_creation_social_media", newUser.getUserId(), buildSocialUserCreationPayload(newUser, avatarUrl).toString()),
                "Send user information to identity service after creating new user from social media"
        );

        SenderRecord<String, String, String> profileCreationRecord = SenderRecord.create(
                new ProducerRecord<>("profile_creation_event", newUser.getUserId(), buildProfileCreationPayload(newUser, avatarUrl, fullName).toString()),
                "Profile creation event for new social login user"
        );

        /// insert user to database
        return r2dbcEntityTemplate.insert(UserCredentials.class)
                .using(newUser)
                .doOnSuccess(userCredentials -> logger.info("|SocialLoginService|socialAccountCreated|userId={}", userCredentials.getUserId()))
                .onErrorMap(throwable -> {
                    logger.error("|SocialLoginService|socialAccountCreationFailed|errorType={}", throwable.getClass().getSimpleName());
                    return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                })
                /// send current social creation event to kafka
                .thenMany(kafkaSender.send(Mono.just(socialUserCreationRecord))
                        .doOnComplete(() -> logger.info("|SocialLoginService| Successfully sent user creation event to Kafka for userId: {}", newUser.getUserId()))
                        .onErrorMap(e -> {
                            logger.error("|SocialLoginService|user_creation_social_media_failed|userId={}|errorType={}", newUser.getUserId(), e.getClass().getSimpleName());
                            return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                        }))
                /// send profile creation event so user profile is created like normal registration
                .thenMany(kafkaSender.send(Mono.just(profileCreationRecord))
                        .doOnComplete(() -> logger.info("|SocialLoginService| Successfully sent profile_creation_event to Kafka for userId: {}", newUser.getUserId()))
                        .onErrorMap(e -> {
                            logger.error("|SocialLoginService|profile_creation_event_failed|userId={}|errorType={}", newUser.getUserId(), e.getClass().getSimpleName());
                            return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                        }))
                .then()
                .thenReturn(oAuth2User);
    }

    private JsonObject buildSocialUserCreationPayload(UserCredentials userCredentials, String avatarUrl) {
        JsonObject payload = new JsonObject();
        payload.addProperty("username", userCredentials.getUsername());
        payload.addProperty("email", userCredentials.getEmail());
        payload.addProperty("provider", userCredentials.getProvider());
        payload.addProperty("providerId", userCredentials.getProviderId());
        payload.addProperty("avatarUrl", avatarUrl);
        return payload;
    }

    private JsonObject buildProfileCreationPayload(UserCredentials userCredentials, String avatarUrl, String fullName) {
        JsonObject payload = buildSocialUserCreationPayload(userCredentials, avatarUrl);
        payload.addProperty("userId", userCredentials.getUserId());
        payload.addProperty("fullName", firstNonBlank(fullName, userCredentials.getUsername(), userCredentials.getEmail(), "Social user"));
        payload.addProperty("phoneNumber", "");
        payload.addProperty("dob", "");
        payload.addProperty("sex", "");
        payload.addProperty("livingIn", "");
        payload.addProperty("hometown", "");
        payload.add("hobbyList", new JsonArray());
        payload.addProperty("role", userCredentials.getUserRole());
        return payload;
    }
    /// utils
    private Mono<String> fetchGithubEmail(OAuth2UserRequest oAuth2UserRequest) {
        String accessToken = oAuth2UserRequest.getAccessToken().getTokenValue();
        
        return webClient.get()
                .uri("https://api.github.com/user/emails")
                .header("Authorization", "token " + accessToken)  // GitHub uses "token" not "Bearer"
                .header("Accept", "application/vnd.github.v3+json")
                .retrieve()
                .bodyToFlux(Map.class)
                .filter(emailObj -> {
                    Boolean verified = (Boolean) emailObj.get("verified");
                    Boolean primary = (Boolean) emailObj.get("primary");  // Also check for primary email
                    return (verified != null && verified) || (primary != null && primary);
                })
                .map(emailObj -> (String) emailObj.get("email"))
                .next()
                .doOnNext(email -> logger.info("|SocialLoginService|githubEmailResolved=true"))
                .doOnError(e -> logger.warn("|SocialLoginService|githubEmailLookupFailed|errorType={}", e.getClass().getSimpleName()))
                .onErrorResume(e -> {
                    logger.warn("|SocialLoginService| Failed to fetch email from GitHub, will use null");
                    return Mono.empty();
                });
    }

    private String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.isBlank()) {
                return value.trim();
            }
        }
        return "";
    }

    private String generateUsername(String email) {
        if (email != null && email.contains("@")) {
            int curCount = count.incrementAndGet();
            return email.substring(0, email.indexOf("@")) + curCount;
        }
        return "user_" + UUID.randomUUID().toString().substring(0, 8);
    }
}
