package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.PostItemResponse;
import com.dauducbach.clone.modules.post.dto.response.PostMediaResponse;
import com.dauducbach.clone.modules.user.publicapi.UserIdentity;
import com.dauducbach.clone.modules.user.publicapi.UserIdentityQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RichPostSearchServiceTest {
    @Mock
    PostSearchService postSearchService;
    @Mock
    PostDetailQueryService postDetailQueryService;
    @Mock
    UserIdentityQuery userIdentityQuery;

    @Test
    void returnsAuthorAvatarAndAtMostThreeSearchThumbnails() {
        RichPostSearchService service = new RichPostSearchService(
                postSearchService,
                postDetailQueryService,
                userIdentityQuery
        );
        PostDetailResponse detail = new PostDetailResponse(
                "post-1",
                "author-1",
                "bach",
                "Dau Duc Bach",
                "A searchable caption",
                null,
                List.of("spring"),
                "4:3",
                "APPROVED",
                null,
                null,
                null,
                null,
                List.of(item(1), item(2), item(3), item(4)),
                Instant.parse("2026-07-28T00:00:00Z"),
                Instant.parse("2026-07-28T00:00:00Z")
        );

        when(postSearchService.searchPosts("spring", 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of("post-1"), 0, 1, 20)));
        when(postDetailQueryService.getPostDetail("post-1", MediaDisplayType.SEARCH_THUMBNAIL))
                .thenReturn(Mono.just(detail));
        when(userIdentityQuery.findIdentity("author-1"))
                .thenReturn(Mono.just(new UserIdentity("author-1", "bach", "Dau Duc Bach", "https://cdn/avatar.jpg")));

        StepVerifier.create(service.search("spring", 0, 20))
                .assertNext(page -> {
                    assertThat(page.content()).hasSize(1);
                    assertThat(page.content().getFirst().authorAvatarUrl()).isEqualTo("https://cdn/avatar.jpg");
                    assertThat(page.content().getFirst().items())
                            .extracting(PostItemResponse::orderNumber)
                            .containsExactly(1, 2, 3);
                    assertThat(page.content().getFirst().totalMediaItems()).isEqualTo(4);
                })
                .verifyComplete();

        verify(postDetailQueryService).getPostDetail("post-1", MediaDisplayType.SEARCH_THUMBNAIL);
    }

    private PostItemResponse item(int order) {
        return new PostItemResponse(
                "item-" + order,
                order,
                "caption-" + order,
                new PostMediaResponse(
                        "asset-" + order,
                        "public-" + order,
                        "jpg",
                        "image",
                        "https://cdn/item-" + order + ".jpg",
                        "https://cdn/item-" + order + ".jpg",
                        "item-" + order,
                        1200,
                        900
                ),
                null
        );
    }
}
