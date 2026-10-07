package com.dauducbach.clone.modules.post.comments.http;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.post.dto.request.CommentCreateRequest;
import com.dauducbach.clone.modules.post.dto.request.CommentUpdateRequest;
import com.dauducbach.clone.modules.post.dto.response.CommentCreateResponse;
import com.dauducbach.clone.modules.post.entity.Comment;
import com.dauducbach.clone.modules.post.comments.application.CommentWriteService;
import com.dauducbach.clone.modules.post.comments.query.CommentReadService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/comments")
public class CommentController {
    private final CommentWriteService commentWriteService;
    private final CommentReadService commentReadService;

    @PostMapping
    public Mono<ResponseEntity<ApiResponse<CommentCreateResponse>>> createComment(@RequestBody CommentCreateRequest request, Authentication authentication) {
        request.setUserId(ActorIdentity.require(authentication.getName(), request.getUserId()));
        return commentWriteService.createComment(request)
                .map(response -> ResponseEntity.accepted().body(ApiResponse.<CommentCreateResponse>builder()
                        .message(response.getMessage())
                        .result(response)
                        .build()));
    }

    @PutMapping
    public Mono<ApiResponse<Comment>> updateComment(@RequestBody CommentUpdateRequest request, Authentication authentication) {
        request.setUserId(ActorIdentity.require(authentication.getName(), request.getUserId()));
        return commentWriteService.updateComment(request)
                .map(updated -> ApiResponse.<Comment>builder()
                        .message("Comment updated successfully")
                        .result(updated)
                        .build());
    }

    @DeleteMapping("/{commentId}")
    public Mono<ApiResponse<String>> deleteComment(@PathVariable String commentId, Authentication authentication) {
        return commentWriteService.deleteComment(commentId, authentication.getName())
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("Comment deleted successfully")
                        .result("Deleted commentId: " + commentId)
                        .build()));
    }

    @GetMapping("/{commentId}")
    public Mono<ApiResponse<Comment>> getCommentById(@PathVariable String commentId) {
        return commentReadService.getCommentById(commentId)
                .map(comment -> ApiResponse.<Comment>builder()
                        .message("Comment retrieved successfully")
                        .result(comment)
                        .build());
    }

    @GetMapping("/post/{postId}/page")
    public Mono<ApiResponse<PageResponse<Comment>>> getRootCommentPage(
            @PathVariable String postId,
            @RequestParam(required = false) String viewerId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size
    ) {
        Mono<PageResponse<Comment>> source = viewerId == null || viewerId.isBlank()
                ? commentReadService.getRootCommentsPage(postId, page, size)
                : commentReadService.getRootCommentsPage(postId, viewerId, page, size);
        return source.map(result -> ApiResponse.<PageResponse<Comment>>builder()
                        .message("Post comments fetched successfully")
                        .result(result)
                        .build());
    }

    @GetMapping("/post/{postId}")
    public Flux<Comment> getRootComments(@PathVariable String postId,
                                         @RequestParam(required = false) String viewerId,
                                         @RequestParam(defaultValue = "0") int page,
                                         @RequestParam(defaultValue = "10") int size) {
        return viewerId == null || viewerId.isBlank()
                ? commentReadService.getRootComments(postId, page, size)
                : commentReadService.getRootComments(postId, viewerId, page, size);
    }

    @GetMapping("/parent/{parentId}")
    public Flux<Comment> getChildComments(@PathVariable String parentId,
                                          @RequestParam(required = false) String viewerId,
                                          @RequestParam(defaultValue = "0") int page,
                                          @RequestParam(defaultValue = "10") int size) {
        return viewerId == null || viewerId.isBlank()
                ? commentReadService.getChildComments(parentId, page, size)
                : commentReadService.getChildComments(parentId, viewerId, page, size);
    }
    @GetMapping("/user/{userId}")
    public Mono<ApiResponse<PageResponse<Comment>>> getCommentsByUserId(@PathVariable String userId,
                                                                        @RequestParam(defaultValue = "0") int page,
                                                                        @RequestParam(defaultValue = "10") int size) {
        return commentReadService.getCommentsByUserId(userId, page, size)
                .map(response -> ApiResponse.<PageResponse<Comment>>builder()
                        .message("User comments fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/user/{userId}/posts")
    public Mono<ApiResponse<PageResponse<String>>> getCommentedPostIdsByUserId(@PathVariable String userId,
                                                                               @RequestParam(defaultValue = "0") int page,
                                                                               @RequestParam(defaultValue = "10") int size) {
        return commentReadService.getCommentedPostIdsByUserId(userId, page, size)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Commented posts fetched successfully")
                        .result(response)
                        .build());
    }

    @GetMapping("/post/{postId}/count")
    public Mono<ApiResponse<Long>> countCommentsByPostId(@PathVariable String postId) {
        return commentReadService.countCommentsByPostId(postId)
                .map(count -> ApiResponse.<Long>builder()
                        .message("Post comment count fetched successfully")
                        .result(count)
                        .build());
    }

    @GetMapping("/parent/{parentId}/count")
    public Mono<ApiResponse<Long>> countRepliesByParentId(@PathVariable String parentId) {
        return commentReadService.countRepliesByParentId(parentId)
                .map(count -> ApiResponse.<Long>builder()
                        .message("Reply count fetched successfully")
                        .result(count)
                        .build());
    }
}
