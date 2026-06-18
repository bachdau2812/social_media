package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.UserPhoneRequest;
import com.dauducbach.clone.modules.user.entity.UserPhone;
import com.dauducbach.clone.modules.user.service.UserPhoneService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-phones")
public class UserPhoneController {
    private final UserPhoneService userPhoneService;

    /// Tạo mới UserPhone
    @PostMapping
    public Mono<ApiResponse<UserPhone>> createUserPhone(@Valid @RequestBody UserPhoneRequest request) {
        return userPhoneService.createUserPhone(request)
                .map(createdPhone -> ApiResponse.<UserPhone>builder()
                        .message("UserPhone created successfully")
                        .result(createdPhone)
                        .build());
    }

    /// Lấy UserPhone theo ID
    @GetMapping("/{id}")
    public Mono<ApiResponse<UserPhone>> getUserPhoneById(@PathVariable String id) {
        return userPhoneService.getUserPhoneById(id)
                .map(phone -> ApiResponse.<UserPhone>builder()
                        .message("UserPhone retrieved successfully")
                        .result(phone)
                        .build());
    }

    /// Lấy danh sách UserPhone của user
    @GetMapping("/user/{userId}")
    public Flux<UserPhone> getUserPhonesByUserId(@PathVariable String userId) {
        return userPhoneService.getUserPhonesByUserId(userId);
    }

    /// Xóa UserPhone
    @DeleteMapping("/{id}")
    public Mono<ApiResponse<String>> deleteUserPhone(@PathVariable String id) {
        return userPhoneService.deleteUserPhone(id)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("UserPhone deleted successfully")
                        .result("UserPhone with ID: " + id + " has been deleted")
                        .build()));
    }
}
