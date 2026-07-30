package com.dauducbach.clone.modules.post.service;

import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import org.junit.jupiter.api.Test;
import org.springframework.data.elasticsearch.core.ReactiveElasticsearchOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostFeedQueryServiceTest {
    @Test
    void approvedLookupUsesSourceQueryThatExcludesArchivedPosts() {
        PostDetailsRepository repository = mock(PostDetailsRepository.class);
        ReactiveElasticsearchOperations elasticsearch = mock(ReactiveElasticsearchOperations.class);
        PostFeedQueryService service = new PostFeedQueryService(repository, elasticsearch);
        PostDetails post = PostDetails.builder()
                .postId("post-1")
                .validateStatus("APPROVED")
                .build();

        when(repository.findApprovedFeedEligibleById("post-1")).thenReturn(Mono.just(post));

        StepVerifier.create(service.getApprovedPostById("post-1"))
                .expectNext(post)
                .verifyComplete();

        verify(repository).findApprovedFeedEligibleById("post-1");
    }
}
