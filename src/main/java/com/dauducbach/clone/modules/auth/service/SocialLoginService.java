package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
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
import java.util.Objects;
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
    KafkaSender<String, Object> kafkaSender;
    PasswordEncoder passwordEncoder;
    WebClient webClient;

    @Override
    public Mono<OAuth2User> loadUser(OAuth2UserRequest userRequest) {
        ReactiveOAuth2UserService<OAuth2UserRequest, OAuth2User> oauth2UserService = new DefaultReactiveOAuth2UserService();

        return oauth2UserService.loadUser(userRequest)
                .doOnSuccess(oAuth2User -> logger.info("|SocialLoginService| Successfully loaded user from provider: {}", oAuth2User.getAttributes()))
                .onErrorMap(throwable -> {
                    logger.info("|SocialLoginService| Error loading user from provider: {}", throwable.getMessage());
                    return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                })
                .flatMap(oAuth2User -> processUser(userRequest, oAuth2User))
                .doOnSuccess(oAuth2User -> logger.info("|SocialLoginService|loadUser|process user success from provider: {}", oAuth2User.getAttributes()));
    }

    public Mono<OAuth2User> processUser(OAuth2UserRequest oAuth2UserRequest, OAuth2User oAuth2User) {
        logger.info("|SocialLoginService|processUser|Processing user from provider: {}", oAuth2User.getAttributes());

        /// extract provider common information
        String email = oAuth2User.getAttribute("email");
        String displayName = oAuth2User.getAttribute("name");
        String provider = oAuth2UserRequest.getClientRegistration().getRegistrationId();

        logger.info("|SocialLoginService| Processing user from provider: email={}, displayName={}, provider={}", email, displayName, provider);

        /// extract provider specific information
        String providerId = extractProviderId(oAuth2User, provider);
        String avatarUrl = extractAvatarUrl(oAuth2User, provider);

        logger.info("|SocialLoginService| Extracted provider-specific information: providerId={}, avatarUrl={}", providerId, avatarUrl);

        /// If email is null and provider is GitHub, try to fetch from GitHub API
        Mono<String> emailMono = email != null ? Mono.just(email) : 
            ("github".equals(provider) ? fetchGithubEmail(oAuth2UserRequest) : Mono.just(null));

        return emailMono.flatMap(fetchedEmail -> {
            /// validate required information
            if (fetchedEmail == null || providerId == null) {
                logger.error("|SocialLoginService| Missing required user information from provider: email={}, providerId={}", fetchedEmail, providerId);
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
                                            logger.error("|SocialLoginService| Email already linked to another account: email={}", fetchedEmail);
                                            return Mono.error(new AppException(ErrorCode.EMAIL_ALREADY_LINKED));
                                        } else {
                                            /// create new account for this user
                                            return createNewUser(fetchedEmail, provider, providerId, avatarUrl, oAuth2User);
                                        }
                                    });
                        }

                        logger.info("|SocialLoginService| Found existing user with providerId: {}", providerId);
                        return Mono.just(oAuth2User);
                    });
        });
    }

    public Mono<OAuth2User> createNewUser(String email, String provider, String providerId, String avatarUrl, OAuth2User oAuth2User) {
        UserCredentials newUser = UserCredentials.builder()
                .userId(UUID.randomUUID().toString())
                .username(generateUsername(email))
                .email(email)
                // Default password is user's email
                .userPassword(passwordEncoder.encode(email))
                .provider(provider)
                .providerId(providerId)
                .build();

        /// Broadcast new user creation from social media
        JsonObject payload = new JsonObject();
        payload.addProperty("username", newUser.getUsername());
        payload.addProperty("email", newUser.getEmail());
        payload.addProperty("provider", newUser.getProvider());
        payload.addProperty("providerId", newUser.getProviderId());
        payload.addProperty("avatarUrl", avatarUrl);

        ProducerRecord<String, Object> record = new ProducerRecord<>("user_creation_social_media", newUser.getUserId(), payload.toString());
        SenderRecord<String, Object, String> senderRecord = SenderRecord.create(record, "Send user information to identity service after creating new user from social media");

        /// insert user to database
        return r2dbcEntityTemplate.insert(UserCredentials.class)
                .using(newUser)
                .doOnSuccess(userCredentials -> logger.info("|SocialLoginService| Successfully created new user from social media: {}", userCredentials))
                .onErrorMap(throwable -> {
                    logger.info("|SocialLoginService| Error creating new user from social media: {}", throwable.getMessage());
                    return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                })
                /// send event to kafka
                .thenMany(kafkaSender.send(Mono.just(senderRecord)))
                        .doOnComplete(() -> logger.info("|SocialLoginService| Successfully sent user creation event to Kafka for userId: {}", newUser.getUserId()))
                        .onErrorMap(e -> {
                            logger.error("|SocialLoginService| Failed to send user creation event to Kafka for userId: {}, error: {}", newUser.getUserId(), e.getMessage());
                            return new AppException(ErrorCode.LOAD_USER_FROM_SOCIAL_MEDIA_FAIL);
                        })
                .then()
                .thenReturn(oAuth2User);
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
                .doOnNext(email -> logger.info("|SocialLoginService| Successfully fetched email from GitHub: {}", email))
                .doOnError(e -> logger.error("|SocialLoginService| Error fetching email from GitHub: {}", e.getMessage()))
                .onErrorResume(e -> {
                    logger.warn("|SocialLoginService| Failed to fetch email from GitHub, will use null");
                    return Mono.empty();
                });
    }

    private String extractProviderId(OAuth2User oAuth2User, String provider) {
        return switch (provider) {
            case "google" -> oAuth2User.getAttribute("sub"); // String
            case "facebook" -> (String) Objects.requireNonNull(oAuth2User.getAttribute("id"));
            case "github" -> {
                Integer githubId = oAuth2User.getAttribute("id");
                yield githubId != null ? githubId.toString() : null;
            }
            default -> null;
        };
    }

    private String extractAvatarUrl(OAuth2User oAuth2User, String provider) {
        switch (provider) {
            case "google":
                return oAuth2User.getAttribute("picture");
            case "facebook":
                Map<String, Object> picture = oAuth2User.getAttribute("picture");
                if (picture != null) {
                    Map<String, Object> data = (Map<String, Object>) picture.get("data");
                    if (data != null) {
                        return (String) data.get("url");
                    }
                }
                return null;
            case "github":
                return oAuth2User.getAttribute("avatar_url");
            default:
                return null;
        }
    }

    private String generateUsername(String email) {
        if (email != null && email.contains("@")) {
            int curCount = count.incrementAndGet();
            return email.substring(0, email.indexOf("@")) + curCount;
        }
        return "user_" + UUID.randomUUID().toString().substring(0, 8);
    }
}
