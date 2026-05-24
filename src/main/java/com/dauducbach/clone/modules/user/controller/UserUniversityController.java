package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.UserUniversityRequest;
import com.dauducbach.clone.modules.user.entity.UserUniversity;
import com.dauducbach.clone.modules.user.service.UserUniversityService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-universities")
public class UserUniversityController {
    private final UserUniversityService userUniversityService;

    /// Tạo mới UserUniversity
    @PostMapping
    public Mono<ApiResponse<UserUniversity>> createUserUniversity(@Valid @RequestBody UserUniversityRequest request) {
        return userUniversityService.createUserUniversity(request)
                .map(createdUniversity -> ApiResponse.<UserUniversity>builder()
                        .message("UserUniversity created successfully")
                        .result(createdUniversity)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserUniversity>builder()
                        .message("Error creating UserUniversity: " + error.getMessage())
                        .build()));
    }

    /// Lấy UserUniversity theo ID
    @GetMapping("/{id}")
    public Mono<ApiResponse<UserUniversity>> getUserUniversityById(@PathVariable String id) {
        return userUniversityService.getUserUniversityById(id)
                .map(university -> ApiResponse.<UserUniversity>builder()
                        .message("UserUniversity retrieved successfully")
                        .result(university)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserUniversity>builder()
                        .message("Error retrieving UserUniversity: " + error.getMessage())
                        .build()));
    }

    /// Lấy danh sách UserUniversity của user
    @GetMapping("/user/{userId}")
    public Flux<UserUniversity> getUserUniversitiesByUserId(
            @PathVariable String userId,
            @RequestParam(required = false, defaultValue = "false") Boolean includeNonPublic) {
        return userUniversityService.getUserUniversitiesByUserId(userId, includeNonPublic);
    }

    /// Xóa UserUniversity
    @DeleteMapping("/{id}")
    public Mono<ApiResponse<String>> deleteUserUniversity(@PathVariable String id) {
        return userUniversityService.deleteUserUniversity(id)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("UserUniversity deleted successfully")
                        .result("UserUniversity with ID: " + id + " has been deleted")
                        .build()))
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error deleting UserUniversity: " + error.getMessage())
                        .build()));
    }
}