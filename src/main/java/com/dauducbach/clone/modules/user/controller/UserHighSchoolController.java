package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.UserHighSchoolRequest;
import com.dauducbach.clone.modules.user.entity.UserHighSchool;
import com.dauducbach.clone.modules.user.service.UserHighSchoolService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-high-schools")
public class UserHighSchoolController {
    private final UserHighSchoolService userHighSchoolService;

    /// Tạo mới UserHighSchool
    @PostMapping
    public Mono<ApiResponse<UserHighSchool>> createUserHighSchool(@Valid @RequestBody UserHighSchoolRequest request) {
        return userHighSchoolService.createUserHighSchool(request)
                .map(createdHighSchool -> ApiResponse.<UserHighSchool>builder()
                        .message("UserHighSchool created successfully")
                        .result(createdHighSchool)
                        .build());
    }

    /// Lấy UserHighSchool theo ID
    @GetMapping("/{id}")
    public Mono<ApiResponse<UserHighSchool>> getUserHighSchoolById(@PathVariable String id) {
        return userHighSchoolService.getUserHighSchoolById(id)
                .map(highSchool -> ApiResponse.<UserHighSchool>builder()
                        .message("UserHighSchool retrieved successfully")
                        .result(highSchool)
                        .build());
    }

    /// Lấy danh sách UserHighSchool của user
    @GetMapping("/user/{userId}")
    public Flux<UserHighSchool> getUserHighSchoolsByUserId(
            @PathVariable String userId,
            @RequestParam(required = false, defaultValue = "false") Boolean includeNonPublic) {
        return userHighSchoolService.getUserHighSchoolsByUserId(userId, includeNonPublic);
    }

    /// Xóa UserHighSchool
    @DeleteMapping("/{id}")
    public Mono<ApiResponse<String>> deleteUserHighSchool(@PathVariable String id) {
        return userHighSchoolService.deleteUserHighSchool(id)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("UserHighSchool deleted successfully")
                        .result("UserHighSchool with ID: " + id + " has been deleted")
                        .build()));
    }
}
