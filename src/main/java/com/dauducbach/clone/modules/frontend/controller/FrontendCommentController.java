package com.dauducbach.clone.modules.frontend.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.frontend.dto.CommentViewResponse;
import com.dauducbach.clone.modules.frontend.service.CommentScreenService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/frontend/comments")
public class FrontendCommentController {
    private final CommentScreenService service;

    @GetMapping("/post/{postId}/page")
    public Mono<ApiResponse<PageResponse<CommentViewResponse>>> getRootComments(
            @PathVariable String postId,
            @RequestParam(required = false) String viewerId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size
    ) {
        return service.getRootComments(postId, viewerId, page, size)
                .map(result -> ApiResponse.<PageResponse<CommentViewResponse>>builder()
                        .message("Post comments with user profiles fetched successfully")
                        .result(result)
                        .build());
    }

    @GetMapping("/parent/{parentId}")
    public Flux<CommentViewResponse> getReplies(
            @PathVariable String parentId,
            @RequestParam(required = false) String viewerId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size
    ) {
        return service.getReplies(parentId, viewerId, page, size);
    }
}