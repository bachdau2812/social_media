package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.infrastructure.service.SemanticVectorSearchService;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.LinkedHashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostSearchServiceTest {
    @Mock
    PostDetailsRepository postDetailsRepository;
    @Mock
    SemanticVectorSearchService semanticVectorSearchService;

    @Test
    void searchPostsFillsSparseDbPageBySemanticSearch() {
        PostSearchService service = new PostSearchService(postDetailsRepository, semanticVectorSearchService);

        when(postDetailsRepository.countSearchApprovedPostIds("spring")).thenReturn(Mono.just(1L));
        when(postDetailsRepository.searchApprovedPostIds(eq("spring"), any(Pageable.class)))
                .thenReturn(Flux.just("post-db-1"));
        when(semanticVectorSearchService.searchPostIds(
                "spring",
                19,
                new LinkedHashSet<>(List.of("post-db-1"))
        )).thenReturn(Mono.just(List.of("post-semantic-1")));

        StepVerifier.create(service.searchPosts(" spring ", 0, 20))
                .assertNext(response -> {
                    assertThat(response.content()).containsExactly("post-db-1", "post-semantic-1");
                    assertThat(response.totalElements()).isEqualTo(2);
                    assertThat(response.pageNumber()).isZero();
                })
                .verifyComplete();

        verify(postDetailsRepository).searchApprovedPostIds(eq("spring"), any(Pageable.class));
    }
}
