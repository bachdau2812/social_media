package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.media.constant.OwnerType;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.dto.request.LikeRequest;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.CommentCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.LikeToggleResponse;
import com.dauducbach.clone.modules.media.dto.response.MediaSignatureResponse;
import com.dauducbach.clone.modules.post.dto.response.PostCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.PostDetailResponse;
import com.dauducbach.clone.modules.post.dto.response.PostNotificationMuteResponse;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.media.entity.Media;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.media.controller.MediaUploadController;
import com.dauducbach.clone.modules.media.service.CloudinarySignatureService;
import com.dauducbach.clone.modules.post.service.comment.CommentService;
import com.dauducbach.clone.modules.post.service.post.LikeService;
import com.dauducbach.clone.modules.media.service.MediaService;
import com.dauducbach.clone.modules.post.service.post.PostSearchService;
import com.dauducbach.clone.modules.post.service.post.PostDetailQueryService;
import com.dauducbach.clone.modules.post.service.post.PostService;
import com.dauducbach.clone.modules.post.service.post.PostSseService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.reactive.server.SecurityMockServerConfigurers.mockAuthentication;

class PostModuleControllerTest {
    PostService postService;
    CommentService commentService;
    LikeService likeService;
    CloudinarySignatureService cloudinarySignatureService;
    MediaService mediaService;
    PostSseService postSseService;
    PostSearchService postSearchService;
    PostDetailQueryService postDetailQueryService;
    WebTestClient client;

    @BeforeEach
    void setUp() {
        postService = mock(PostService.class);
        commentService = mock(CommentService.class);
        likeService = mock(LikeService.class);
        cloudinarySignatureService = mock(CloudinarySignatureService.class);
        mediaService = mock(MediaService.class);
        postSseService = mock(PostSseService.class);
        postSearchService = mock(PostSearchService.class);
        postDetailQueryService = mock(PostDetailQueryService.class);

        client = WebTestClient.bindToController(
                        new PostController(postService, postSearchService, postDetailQueryService),
                        new CommentController(commentService),
                        new LikeController(likeService),
                        new MediaUploadController(cloudinarySignatureService, mediaService),
                        new PostSseController(postSseService)
                )
                .webFilter((exchange, chain) -> chain.filter(exchange.mutate()
                        .principal(Mono.just(new TestingAuthenticationToken("user-1", "n/a")))
                        .build()))
                .build();
    }

    @Test
    void createPostReturnsAcceptedApiResponse() {
        when(postService.createPost(any(PostCreateRequest.class)))
                .thenReturn(Mono.just(PostCreateResponse.builder()
                        .postId("post-1")
                        .message("Dang doi xu ly va duyet media")
                        .build()));

        client.post()
                .uri("/posts")
                .bodyValue(PostCreateRequest.builder().userId("user-1").content("hello").build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.postId").isEqualTo("post-1")
                .jsonPath("$.message").isEqualTo("Dang doi xu ly va duyet media");
    }

    @Test
    void updatePostReturnsUpdatedPost() {
        PostDetails post = post("post-1", "user-1");
        when(postService.updatePost(any(PostUpdateRequest.class))).thenReturn(Mono.just(post));
        when(postDetailQueryService.getPostDetail("post-1", MediaDisplayType.POST)).thenReturn(Mono.just(postDetail("post-1")));

        client.put()
                .uri("/posts")
                .bodyValue(PostUpdateRequest.builder().postId("post-1").userId("user-1").content("updated").build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.postId").isEqualTo("post-1");
    }

    @Test
    void searchPostsReturnsPagedPostIds() {
        when(postSearchService.searchPosts("hello", 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of("post-1", "post-2"), 0, 2, 20)));

        client.get()
                .uri("/posts/search?query=hello")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0]").isEqualTo("post-1")
                .jsonPath("$.result.totalElements").isEqualTo(2);
    }

    @Test
    void getPostByIdReturnsPost() {
        when(postDetailQueryService.getPostDetail("post-1", MediaDisplayType.POST)).thenReturn(Mono.just(postDetail("post-1")));

        client.get()
                .uri("/posts/post-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.postId").isEqualTo("post-1");
    }

    @Test
    void getPostsByUserIdReturnsFlux() {
        when(postService.getPostsByUserId("user-1", 0, 10))
                .thenReturn(Flux.just(post("post-1", "user-1"), post("post-2", "user-1")));

        client.get()
                .uri("/posts/user/user-1?page=0&size=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].postId").isEqualTo("post-1")
                .jsonPath("$[1].postId").isEqualTo("post-2");
    }

    @Test
    void deletePostReturnsDeletedMessage() {
        when(postService.deletePostById("post-1", "user-1")).thenReturn(Mono.empty());

        client.delete()
                .uri("/posts/post-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo("Deleted postId: post-1");
    }

    @Test
    void deletePostsByUserReturnsDeletedMessage() {
        when(postService.deletePostsByUserId("user-1")).thenReturn(Mono.empty());

        client.delete()
                .uri("/posts/user/user-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo("Deleted posts for userId: user-1");
    }

    @Test
    void mutePostNotificationsReturnsApiResponse() {
        when(postService.mutePostNotifications("post-1", "user-1"))
                .thenReturn(Mono.just(new PostNotificationMuteResponse("post-1", "user-1", 60)));

        client.post()
                .uri("/posts/post-1/notifications/mute/users/user-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.postId").isEqualTo("post-1")
                .jsonPath("$.result.userId").isEqualTo("user-1")
                .jsonPath("$.result.mutedDays").isEqualTo(60);
    }

    @Test
    void createCommentReturnsAcceptedApiResponse() {
        when(commentService.createComment(any(CommentCreateRequest.class)))
                .thenReturn(Mono.just(CommentCreateResponse.builder()
                        .commentId("comment-1")
                        .message("Comment created")
                        .build()));

        client.post()
                .uri("/comments")
                .bodyValue(CommentCreateRequest.builder().postId("post-1").userId("user-1").content("hello").build())
                .exchange()
                .expectStatus().isAccepted()
                .expectBody()
                .jsonPath("$.result.commentId").isEqualTo("comment-1");
    }

    @Test
    void updateCommentReturnsUpdatedComment() {
        Comment comment = comment("comment-1", "post-1", "user-1", null);
        when(commentService.updateComment(any(CommentUpdateRequest.class))).thenReturn(Mono.just(comment));

        client.put()
                .uri("/comments")
                .bodyValue(CommentUpdateRequest.builder().commentId("comment-1").userId("user-1").content("updated").build())
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.id").isEqualTo("comment-1");
    }

    @Test
    void getCommentByIdReturnsComment() {
        when(commentService.getCommentById("comment-1"))
                .thenReturn(Mono.just(comment("comment-1", "post-1", "user-1", null)));

        client.get()
                .uri("/comments/comment-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.id").isEqualTo("comment-1");
    }

    @Test
    void deleteCommentReturnsDeletedMessage() {
        when(commentService.deleteComment("comment-1", "user-1")).thenReturn(Mono.empty());

        client.delete()
                .uri("/comments/comment-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo("Deleted commentId: comment-1");
    }

    @Test
    void getRootAndChildCommentsReturnFlux() {
        when(commentService.getRootComments("post-1", 0, 10))
                .thenReturn(Flux.just(comment("comment-1", "post-1", "user-1", null)));
        when(commentService.getChildComments("comment-1", 0, 10))
                .thenReturn(Flux.just(comment("comment-2", "post-1", "user-2", "comment-1")));

        client.get()
                .uri("/comments/post/post-1?page=0&size=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].id").isEqualTo("comment-1");

        client.get()
                .uri("/comments/parent/comment-1?page=0&size=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].id").isEqualTo("comment-2");
    }

    @Test
    void newCommentReadApisReturnPageAndCounts() {
        PageResponse<String> postIds = PageResponse.of(List.of("post-2", "post-1"), 0, 2, 10);
        PageResponse<Comment> comments = PageResponse.of(List.of(comment("comment-1", "post-1", "user-1", null)), 0, 1, 10);

        when(commentService.getCommentedPostIdsByUserId("user-1", 0, 10)).thenReturn(Mono.just(postIds));
        when(commentService.getCommentsByUserId("user-1", 0, 10)).thenReturn(Mono.just(comments));
        when(commentService.countCommentsByPostId("post-1")).thenReturn(Mono.just(5L));
        when(commentService.countRepliesByParentId("comment-1")).thenReturn(Mono.just(2L));

        client.get().uri("/comments/user/user-1/posts?page=0&size=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0]").isEqualTo("post-2");

        client.get().uri("/comments/user/user-1?page=0&size=10")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0].id").isEqualTo("comment-1");

        client.get().uri("/comments/post/post-1/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo(5);

        client.get().uri("/comments/parent/comment-1/count")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo(2);
    }

    @Test
    void likeApisReturnExpectedResponses() {
        when(likeService.like(eq("user-1"), any(LikeRequest.class)))
                .thenReturn(Mono.just(new LikeToggleResponse("post-1", "POST", true, "like-1")));
        when(likeService.hasLiked("user-1", "post-1", "POST")).thenReturn(Mono.just(true));
        when(likeService.countLikes("post-1", "POST")).thenReturn(Mono.just(7L));
        when(likeService.getLikedTargets("user-1", "POST", 0, 20))
                .thenReturn(Mono.just(PageResponse.of(List.of("post-1"), 0, 1, 20)));

        client.post().uri("/likes/users/user-1")
                .bodyValue(new LikeRequest("post-1", "POST"))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.likeId").isEqualTo("like-1")
                .jsonPath("$.result.liked").isEqualTo(true);

        client.get().uri("/likes/users/user-1/status?targetId=post-1&targetType=POST")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo(true);

        client.get().uri("/likes/count?targetId=post-1&targetType=POST")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result").isEqualTo(7);

        client.get().uri("/likes/users/user-1/targets?targetType=POST&page=0&size=20")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.content[0]").isEqualTo("post-1");
    }

    @Test
    void mediaApisReturnSignatureAndMedia() {
        Media media = Media.builder()
                .assetId("asset-1")
                .publicId("public-1")
                .ownerId("post-1")
                .ownerType(OwnerType.POST)
                .secureUrl("https://cdn.example/image.jpg")
                .build();

        when(cloudinarySignatureService.generateSignature())
                .thenReturn(MediaSignatureResponse.builder()
                        .signature("sig")
                        .timestamp(1L)
                        .apiKey("api-key")
                        .folder("social_network_posts")
                        .uploadPreset("ml_default")
                        .build());
        when(mediaService.getByPublicId("public-1")).thenReturn(Mono.just(media));
        when(mediaService.getByOwnerId("post-1", OwnerType.POST)).thenReturn(Flux.just(media));

        client.get().uri("/media/signature")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.signature").isEqualTo("sig");

        client.get().uri("/media/public/public-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.result.assetId").isEqualTo("asset-1");

        client.get().uri("/media/owner/post-1?ownerType=POST")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$[0].assetId").isEqualTo("asset-1");
    }

    @Test
    void sseEndpointReturnsEventStream() {
        when(postSseService.subscribe("user-1"))
                .thenReturn(Flux.just(ServerSentEvent.builder("payload").event("post_success_event").build()));

        client.get()
                .uri("/posts/sse/user-1")
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueMatches("Content-Type", "text/event-stream.*");
    }

    private PostDetailResponse postDetail(String postId) {
        return new PostDetailResponse(postId, "user-1", "content", "PUBLIC", "PUBLIC", null, List.of(), "APPROVED", null, null, 0L, 0L, null, List.of(), Instant.now(), Instant.now());
    }

    private PostDetails post(String postId, String userId) {
        return PostDetails.builder()
                .postId(postId)
                .userId(userId)
                .content("content")
                .createdAt(Instant.parse("2026-06-08T00:00:00Z"))
                .updatedAt(Instant.parse("2026-06-08T00:00:00Z"))
                .validateStatus("APPROVED")
                .build();
    }

    private Comment comment(String id, String postId, String userId, String parentId) {
        return Comment.builder()
                .id(id)
                .postId(postId)
                .userId(userId)
                .parentId(parentId)
                .content("content")
                .commentType("TEXT")
                .timestamp(Instant.parse("2026-06-08T00:00:00Z"))
                .build();
    }
}

