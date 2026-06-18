package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.PostCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.PostNotificationMuteResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.service.PostSearchService;
import com.dauducbach.clone.modules.post.service.PostService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/posts")
public class PostController {
    private final PostService postService;
    private final PostSearchService postSearchService;

    @PostMapping
    public Mono<ResponseEntity<ApiResponse<PostCreateResponse>>> createPost(@RequestBody PostCreateRequest request) {
        return postService.createPost(request)
                .map(response -> ResponseEntity.accepted().body(ApiResponse.<PostCreateResponse>builder()
                        .message(response.getMessage())
                        .result(response)
                        .build()));
    }

    @PutMapping
    public Mono<ApiResponse<PostDetails>> updatePost(@RequestBody PostUpdateRequest request) {
        return postService.updatePost(request)
                .map(updated -> ApiResponse.<PostDetails>builder()
                        .message("Post updated successfully")
                        .result(updated)
                        .build());
    }

    @GetMapping("/search")
    public Mono<ApiResponse<PageResponse<String>>> searchPosts(@RequestParam String query,
                                                               @RequestParam(defaultValue = "0") int page,
                                                               @RequestParam(defaultValue = "20") int limit) {
        return postSearchService.searchPosts(query, page, limit)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Posts searched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/{postId}")
    public Mono<ApiResponse<PostDetails>> getPostById(@PathVariable String postId) {
        return postService.getPostById(postId)
                .map(post -> ApiResponse.<PostDetails>builder()
                        .message("Post retrieved successfully")
                        .result(post)
                        .build());
    }

    @GetMapping("/user/{userId}")
    public Flux<PostDetails> getPostsByUserId(@PathVariable String userId,
                                              @RequestParam(defaultValue = "0") int page,
                                              @RequestParam(defaultValue = "10") int size) {
        return postService.getPostsByUserId(userId, page, size);
    }

    @PostMapping("/{postId}/notifications/mute/users/{userId}")
    public Mono<ApiResponse<PostNotificationMuteResponse>> mutePostNotifications(@PathVariable String postId,
                                                                                 @PathVariable String userId) {
        return postService.mutePostNotifications(postId, userId)
                .map(response -> ApiResponse.<PostNotificationMuteResponse>builder()
                        .message("Post notifications muted successfully")
                        .result(response)
                        .build());
    }

    @DeleteMapping("/{postId}")
    public Mono<ApiResponse<String>> deletePost(@PathVariable String postId) {
        return postService.deletePostById(postId)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("Post deleted successfully")
                        .result("Deleted postId: " + postId)
                        .build()));
    }

    @DeleteMapping("/user/{userId}")
    public Mono<ApiResponse<String>> deletePostsByUserId(@PathVariable String userId) {
        return postService.deletePostsByUserId(userId)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("User posts deleted successfully")
                        .result("Deleted posts for userId: " + userId)
                        .build()));
    }
}


