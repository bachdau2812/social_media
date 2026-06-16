package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.post.dto.request.PostCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.PostUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.PostCreateResponse;
import com.dauducbach.clone.modules.post.dto.response.PostNotificationMuteResponse;
import com.dauducbach.clone.modules.post.entity.PostDetails;
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
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<PostDetails>builder()
                        .message("Error updating post: " + error.getMessage())
                        .build()));
    }

    @GetMapping("/{postId}")
    public Mono<ApiResponse<PostDetails>> getPostById(@PathVariable String postId) {
        return postService.getPostById(postId)
                .map(post -> ApiResponse.<PostDetails>builder()
                        .message("Post retrieved successfully")
                        .result(post)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<PostDetails>builder()
                        .message("Error retrieving post: " + error.getMessage())
                        .build()));
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
                        .build()))
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error deleting post: " + error.getMessage())
                        .build()));
    }

    @DeleteMapping("/user/{userId}")
    public Mono<ApiResponse<String>> deletePostsByUserId(@PathVariable String userId) {
        return postService.deletePostsByUserId(userId)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("User posts deleted successfully")
                        .result("Deleted posts for userId: " + userId)
                        .build()))
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error deleting posts: " + error.getMessage())
                        .build()));
    }
}


