package com.dauducbach.clone.modules.user.service;

import com.dauducbach.clone.modules.user.entity.UserDetailVector;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.repositoty.UserDetailsRepository;
import com.dauducbach.clone.modules.user.repositoty.UserHighSchoolRepository;
import com.dauducbach.clone.modules.user.repositoty.UserJobRepository;
import com.dauducbach.clone.modules.user.repositoty.UserUniversityRepository;
import com.dauducbach.clone.utils.GetVectorEmbedding;
import com.dauducbach.clone.utils.GsonUtils;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserProfileVectorRefreshListenerTest {
    @Mock
    UserDetailsRepository userDetailsRepository;
    @Mock
    UserJobRepository userJobRepository;
    @Mock
    UserHighSchoolRepository userHighSchoolRepository;
    @Mock
    UserUniversityRepository userUniversityRepository;
    @Mock
    ReactiveElasticsearchOperations elasticsearchOperations;
    @Mock
    GetVectorEmbedding getVectorEmbedding;

    @Test
    void buildProfileTextAggregatesUserProfileComponents() {
        UserProfileVectorRefreshListener listener = newListener();
        mockProfileSources();

        StepVerifier.create(listener.buildProfileText("user-1"))
                .assertNext(text -> {
                    assertThat(text).contains("username: bach");
                    assertThat(text).contains("hobbies: music, backend");
                    assertThat(text).contains("job: Developer, OpenAI");
                    assertThat(text).contains("high_school: Nguyen Trai");
                    assertThat(text).contains("university: HUST, Computer Science");
                })
                .verifyComplete();
    }

    @Test
    void refreshUserVectorEmbedsProfileTextAndReplacesUserVector() {
        UserProfileVectorRefreshListener listener = newListener();
        mockProfileSources();
        UserDetailVector existing = UserDetailVector.builder()
                .userId("user-1")
                .userVector(List.of(0.1, 0.2))
                .userLongTermVector(List.of(0.9, 0.1))
                .build();

        when(getVectorEmbedding.getEmbedding(anyString())).thenReturn(Mono.just(List.of(0.3, 0.7)));
        when(elasticsearchOperations.get("user-1", UserDetailVector.class)).thenReturn(Mono.just(existing));
        when(elasticsearchOperations.save(any(UserDetailVector.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(listener.refreshUserVector("user-1"))
                .verifyComplete();

        ArgumentCaptor<UserDetailVector> captor = ArgumentCaptor.forClass(UserDetailVector.class);
        verify(elasticsearchOperations).save(captor.capture());
        assertThat(captor.getValue().getUserVector()).containsExactly(0.3, 0.7);
        assertThat(captor.getValue().getUserLongTermVector()).containsExactly(0.9, 0.1);
    }

    @Test
    void refreshCreatedUserVectorUsesEventSnapshotWithoutQueryingProfileTables() {
        UserProfileVectorRefreshListener listener = newListener();
        UserDetailVector existing = UserDetailVector.builder()
                .userId("user-1")
                .build();

        when(getVectorEmbedding.getEmbedding(anyString())).thenReturn(Mono.just(List.of(0.4, 0.6)));
        when(elasticsearchOperations.get("user-1", UserDetailVector.class)).thenReturn(Mono.just(existing));
        when(elasticsearchOperations.save(any(UserDetailVector.class))).thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        StepVerifier.create(listener.refreshCreatedUserVector(GsonUtils.fromString("""
                        {"userId":"user-1","username":"bach","hometown":"Ha Noi","livingIn":"Ho Chi Minh","sex":"male","dob":"2000-01-02","hobbyList":["music","backend"]}
                        """)))
                .verifyComplete();

        verify(userDetailsRepository, never()).findById(anyString());
        verify(userJobRepository, never()).findByUserId(anyString());
        verify(userHighSchoolRepository, never()).findByUserId(anyString());
        verify(userUniversityRepository, never()).findByUserId(anyString());
        ArgumentCaptor<UserDetailVector> captor = ArgumentCaptor.forClass(UserDetailVector.class);
        verify(elasticsearchOperations).save(captor.capture());
        assertThat(captor.getValue().getUserVector()).containsExactly(0.4, 0.6);
    }

    private UserProfileVectorRefreshListener newListener() {
        return new UserProfileVectorRefreshListener(
                userDetailsRepository,
                userJobRepository,
                userHighSchoolRepository,
                userUniversityRepository,
                elasticsearchOperations,
                getVectorEmbedding
        );
    }

    private void mockProfileSources() {
        UserDetails details = UserDetails.builder()
                .userId("user-1")
                .username("bach")
                .hometown("Ha Noi")
                .livingIn("Ho Chi Minh")
                .sex("male")
                .dob(LocalDate.of(2000, 1, 2))
                .build();
        details.setHobbyList(List.of("music", "backend"));

        UserJob job = UserJob.builder()
                .id("job-1")
                .userId("user-1")
                .position("Developer")
                .companyName("OpenAI")
                .fromDate(LocalDate.of(2024, 1, 1))
                .build();

        UserHighSchool highSchool = UserHighSchool.builder()
                .id("high-1")
                .userId("user-1")
                .schoolName("Nguyen Trai")
                .isGraduate(true)
                .build();

        UserUniversity university = UserUniversity.builder()
                .id("uni-1")
                .userId("user-1")
                .schoolName("HUST")
                .major("Computer Science")
                .isGraduate(false)
                .build();

        when(userDetailsRepository.findById("user-1")).thenReturn(Mono.just(details));
        when(userJobRepository.findByUserId("user-1")).thenReturn(Flux.just(job));
        when(userHighSchoolRepository.findByUserId("user-1")).thenReturn(Flux.just(highSchool));
        when(userUniversityRepository.findByUserId("user-1")).thenReturn(Flux.just(university));
    }
}
