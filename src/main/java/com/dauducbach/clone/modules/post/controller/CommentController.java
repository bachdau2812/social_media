package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.CommentCreateResponse;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.service.CommentService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/comments")
public class CommentController {
    private final CommentService commentService;

    @PostMapping
    public Mono<ResponseEntity<ApiResponse<CommentCreateResponse>>> createComment(@RequestBody CommentCreateRequest request) {
        return commentService.createComment(request)
                .map(response -> ResponseEntity.accepted().body(ApiResponse.<CommentCreateResponse>builder()
                        .message(response.getMessage())
                        .result(response)
                        .build()));
    }

    @PutMapping
    public Mono<ApiResponse<Comment>> updateComment(@RequestBody CommentUpdateRequest request) {
        return commentService.updateComment(request)
                .map(updated -> ApiResponse.<Comment>builder()
                        .message("Comment updated successfully")
                        .result(updated)
                        .build());
    }

    @DeleteMapping("/{commentId}")
    public Mono<ApiResponse<String>> deleteComment(@PathVariable String commentId) {
        return commentService.deleteComment(commentId)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("Comment deleted successfully")
                        .result("Deleted commentId: " + commentId)
                        .build()));
    }

    @GetMapping("/{commentId}")
    public Mono<ApiResponse<Comment>> getCommentById(@PathVariable String commentId) {
        return commentService.getCommentById(commentId)
                .map(comment -> ApiResponse.<Comment>builder()
                        .message("Comment retrieved successfully")
                        .result(comment)
                        .build());
    }

    @GetMapping("/post/{postId}")
    public Flux<Comment> getRootComments(@PathVariable String postId,
                                         @RequestParam(defaultValue = "0") int page,
                                         @RequestParam(defaultValue = "10") int size) {
        return commentService.getRootComments(postId, page, size);
    }

    @GetMapping("/parent/{parentId}")
    public Flux<Comment> getChildComments(@PathVariable String parentId,
                                          @RequestParam(defaultValue = "0") int page,
                                          @RequestParam(defaultValue = "10") int size) {
        return commentService.getChildComments(parentId, page, size);
    }

    @GetMapping("/user/{userId}")
    public Mono<ApiResponse<PageResponse<Comment>>> getCommentsByUserId(@PathVariable String userId,
                                                                        @RequestParam(defaultValue = "0") int page,
                                                                        @RequestParam(defaultValue = "10") int size) {
        return commentService.getCommentsByUserId(userId, page, size)
                .map(response -> ApiResponse.<PageResponse<Comment>>builder()
                        .message("User comments fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/user/{userId}/posts")
    public Mono<ApiResponse<PageResponse<String>>> getCommentedPostIdsByUserId(@PathVariable String userId,
                                                                               @RequestParam(defaultValue = "0") int page,
                                                                               @RequestParam(defaultValue = "10") int size) {
        return commentService.getCommentedPostIdsByUserId(userId, page, size)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Commented posts fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/post/{postId}/count")
    public Mono<ApiResponse<Long>> countCommentsByPostId(@PathVariable String postId) {
        return commentService.countCommentsByPostId(postId)
                .map(count -> ApiResponse.<Long>builder()
                        .message("Post comment count fetched successfully")
                        .result(count)
                        .build());
    }

    @GetMapping("/parent/{parentId}/count")
    public Mono<ApiResponse<Long>> countRepliesByParentId(@PathVariable String parentId) {
        return commentService.countRepliesByParentId(parentId)
                .map(count -> ApiResponse.<Long>builder()
                        .message("Reply count fetched successfully")
                        .result(count)
                        .build());
    }
}
