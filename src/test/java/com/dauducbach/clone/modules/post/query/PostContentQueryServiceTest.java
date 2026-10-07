package com.dauducbach.clone.modules.post.query;

import com.dauducbach.clone.modules.post.application.PostDetailsCache;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.repository.PostDetailsRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostContentQueryServiceTest {
    @Mock
    PostDetailsRepository postDetailsRepository;
    @Mock
    PostDetailsCache postDetailsCache;

    @Test
    void returnsCachedPostWithoutReadingPersistence() {
        PostDetails post = PostDetails.builder().postId("post-1").userId("user-1").content("cached").build();
        when(postDetailsCache.get("post-1")).thenReturn(Mono.just(post));

        StepVerifier.create(service().findById("post-1"))
                .expectNext(post)
                .verifyComplete();

        verify(postDetailsRepository, never()).findById("post-1");
    }

    @Test
    void fallsBackToPersistenceAndRefreshesTheCache() {
        PostDetails post = PostDetails.builder().postId("post-1").userId("user-1").content("stored").build();
        when(postDetailsCache.get("post-1")).thenReturn(Mono.empty());
        when(postDetailsRepository.findById("post-1")).thenReturn(Mono.just(post));
        when(postDetailsCache.put(post)).thenReturn(Mono.just(true));

        StepVerifier.create(service().findById("post-1"))
                .expectNext(post)
                .verifyComplete();

        verify(postDetailsCache).put(post);
    }

    @Test
    void appliesSafeAuthorPagination() {
        when(postDetailsRepository.findByUserId("user-1", 50, 50))
                .thenReturn(Flux.just(PostDetails.builder().postId("post-1").build()));

        StepVerifier.create(service().findByAuthorId("user-1", 1, 200))
                .expectNextCount(1)
                .verifyComplete();

        verify(postDetailsRepository).findByUserId("user-1", 50, 50);
    }

    private PostContentQueryService service() {
        return new PostContentQueryService(postDetailsRepository, postDetailsCache);
    }
}
