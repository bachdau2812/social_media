package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.modules.user.dto.request.UserDetailsUpdateRequest;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-details")
public class UserDetailsController {
    private final UserDetailsService userDetailsService;

    /// Get UserDetails by userId
    @GetMapping("/{userId}")
    public Mono<ApiResponse<UserDetails>> getUserDetailsById(@PathVariable String userId,
                                                              @RequestHeader(required = false) String traceId) {
        return userDetailsService.getUserDetailsById(userId)
                .map(userDetails -> ApiResponse.<UserDetails>builder()
                        .message("UserDetails retrieved successfully")
                        .traceId(traceId)
                        .result(userDetails)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserDetails>builder()
                        .message("Error retrieving UserDetails: " + error.getMessage())
                        .traceId(traceId)
                        .build()));
    }

    /// Update UserDetails
    @PutMapping("/update")
    public Mono<ApiResponse<UserDetails>> updateUserDetails(@Valid @RequestBody UserDetailsUpdateRequest request,
                                                             @RequestHeader(required = false) String traceId) {
        return userDetailsService.updateUserDetails(request)
                .map(updatedUserDetails -> ApiResponse.<UserDetails>builder()
                        .message("UserDetails updated successfully")
                        .traceId(traceId)
                        .result(updatedUserDetails)
                        .build())
                .onErrorResume(error -> Mono.just(ApiResponse.<UserDetails>builder()
                        .message("Error updating UserDetails: " + error.getMessage())
                        .traceId(traceId)
                        .build()));
    }

    /// Delete UserDetails by userId
    @DeleteMapping("/{userId}")
    public Mono<ApiResponse<String>> deleteUserDetails(@PathVariable String userId,
                                                        @RequestHeader(required = false) String traceId) {
        return userDetailsService.deleteUserDetails(userId)
                .then(Mono.just(ApiResponse.<String>builder()
                        .message("UserDetails deleted successfully")
                        .traceId(traceId)
                        .result("UserDetails with ID: " + userId + " has been deleted")
                        .build()))
                .onErrorResume(error -> Mono.just(ApiResponse.<String>builder()
                        .message("Error deleting UserDetails: " + error.getMessage())
                        .traceId(traceId)
                        .build()));
    }
}