package com.dauducbach.clone.modules.auth.service;

import com.dauducbach.clone.modules.auth.entity.UserCredentials;
import com.dauducbach.clone.modules.auth.repository.UserCredentialsRepository;
import com.dauducbach.clone.utils.GsonUtils;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.userinfo.OAuth2UserRequest;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;
import java.time.Instant;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;
import static org.assertj.core.api.Assertions.*;

class SocialLoginServiceTest {
    final R2dbcEntityTemplate template = mock(R2dbcEntityTemplate.class, RETURNS_DEEP_STUBS);
    final UserCredentialsRepository users = mock(UserCredentialsRepository.class);
    @SuppressWarnings("unchecked") final KafkaSender<String,String> sender = mock(KafkaSender.class);
    final PasswordEncoder encoder = mock(PasswordEncoder.class);
    final OAuth2User user = mock(OAuth2User.class);
    final SocialLoginService service = new SocialLoginService(template, users, sender, encoder, mock(WebClient.class));
    OAuth2UserRequest request() {
        var registration = ClientRegistration.withRegistrationId("google").clientId("id").clientSecret("secret")
                .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE).redirectUri("https://local/callback")
                .authorizationUri("https://provider/authorize").tokenUri("https://provider/token")
                .userInfoUri("https://provider/user").userNameAttributeName("sub").build();
        return new OAuth2UserRequest(registration, new OAuth2AccessToken(OAuth2AccessToken.TokenType.BEARER, "test-token", Instant.now(), Instant.now().plusSeconds(60)));
    }
    void profile() {
        when(user.getAttribute("email")).thenReturn("user@example.test");
        when(user.getAttribute("name")).thenReturn("Social Full Name");
        when(user.getAttribute("sub")).thenReturn("provider-id");
        when(user.getAttribute("picture")).thenReturn("https://provider/avatar");
    }
    @Test void existingOauthLoginDoesNotPublishCreationOrResetHistory() {
        profile(); when(users.existsByProviderId("provider-id")).thenReturn(Mono.just(true));
        StepVerifier.create(service.processUser(request(), user)).expectNext(user).verifyComplete();
        verifyNoInteractions(template, sender, encoder);
    }
    @Test @SuppressWarnings({"rawtypes", "unchecked"})
    void newOauthUserPublishesCommonProfileCreationWithFullNameAndAvatar() {
        profile(); when(users.existsByProviderId("provider-id")).thenReturn(Mono.just(false));
        when(users.existsByEmail("user@example.test")).thenReturn(Mono.just(false));
        when(encoder.encode(anyString())).thenReturn("hash");
        when(template.insert(UserCredentials.class).using(any(UserCredentials.class))).thenAnswer(call -> Mono.just(call.getArgument(0)));
        when(sender.send(any())).thenReturn(Flux.empty());
        StepVerifier.create(service.processUser(request(), user)).expectNext(user).verifyComplete();
        ArgumentCaptor<org.reactivestreams.Publisher> records = ArgumentCaptor.forClass(org.reactivestreams.Publisher.class);
        verify(sender, times(2)).send(records.capture());
        SenderRecord record = (SenderRecord) Flux.from(records.getAllValues().get(1)).blockFirst();
        assertThat(record.topic()).isEqualTo("profile_creation_event");
        var payload = GsonUtils.fromString((String) record.value());
        assertThat(payload.get("userId").getAsString()).isEqualTo(record.key());
        assertThat(payload.get("fullName").getAsString()).isEqualTo("Social Full Name");
        assertThat(payload.get("avatarUrl").getAsString()).isEqualTo("https://provider/avatar");
    }
}
