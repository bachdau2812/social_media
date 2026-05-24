package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.UserJobRequest;
import com.dauducbach.clone.modules.user.dto.request.UserJobUpdateRequest;
import com.dauducbach.clone.modules.user.entity.UserJob;
import com.dauducbach.clone.modules.user.service.UserJobService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-jobs")
public class UserJobController {
    private final UserJobService userJobService;

    /// Tạo mới UserJob
    @PostMapping
    public Mono<ApiResponse<UserJob>> createUserJob(@Valid @RequestBody UserJobRequest request) {
        return userJobService.createUserJob(request)
                .map(createdJob -> ApiResponse.<UserJob>builder()
                        .message("UserJob created successfully")
                        .result(createdJob)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserJob>builder()
                        .message("Error creating UserJob: " + error.getMessage())
                        .build()));
    }

    /// Cập nhật UserJob
    @PutMapping
    public Mono<ApiResponse<UserJob>> updateUserJob(@Valid @RequestBody UserJobUpdateRequest request) {
        return userJobService.updateUserJob(request)
                .map(updatedJob -> ApiResponse.<UserJob>builder()
                        .message("UserJob updated successfully")
                        .result(updatedJob)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserJob>builder()
                        .message("Error updating UserJob: " + error.getMessage())
                        .build()));
    }

    /// Lấy UserJob theo ID
    @GetMapping("/{id}")
    public Mono<ApiResponse<UserJob>> getUserJobById(@PathVariable String id) {
        return userJobService.getUserJobById(id)
                .map(job -> ApiResponse.<UserJob>builder()
                        .message("UserJob retrieved successfully")
                        .result(job)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserJob>builder()
                        .message("Error retrieving UserJob: " + error.getMessage())
                        .build()));
    }

    /// Lấy danh sách UserJob của user
    @GetMapping("/user/{userId}")
    public Flux<UserJob> getUserJobsByUserId(
            @PathVariable String userId,
            @RequestParam(required = false, defaultValue = "false") Boolean includeNonPublic) {
        return userJobService.getUserJobsByUserId(userId, includeNonPublic);
    }

    /// Xóa UserJob
    @DeleteMapping("/{id}")
    public Mono<ApiResponse<String>> deleteUserJob(@PathVariable String id) {
        return userJobService.deleteUserJob(id)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("UserJob deleted successfully")
                        .result("UserJob with ID: " + id + " has been deleted")
                        .build()))
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error deleting UserJob: " + error.getMessage())
                        .build()));
    }
}