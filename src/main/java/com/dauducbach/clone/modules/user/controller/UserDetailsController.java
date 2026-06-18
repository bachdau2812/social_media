package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.user.dto.request.UserDetailsUpdateRequest;
import com.dauducbach.clone.modules.user.entity.UserDetails;
import com.dauducbach.clone.modules.user.service.UserDetailsService;
import com.dauducbach.clone.modules.user.service.UserSearchService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/user-details")
public class UserDetailsController {
    private final UserDetailsService userDetailsService;
    private final UserSearchService userSearchService;

    @GetMapping("/search")
    public Mono<ApiResponse<PageResponse<String>>> searchUsers(@RequestParam String query,
                                                               @RequestParam(required = false) String filter,
                                                               @RequestParam(defaultValue = "0") int page,
                                                               @RequestParam(defaultValue = "20") int limit,
                                                               @RequestHeader(required = false) String traceId) {
        return userSearchService.searchUsers(query, filter, page, limit)
                .map(response -> ApiResponse.<PageResponse<String>>builder()
                        .message("Users searched successfully")
                        .traceId(traceId)
                        .result(response)
                        .build());
    }

    /// Get UserDetails by userId
    @GetMapping("/{userId}")
    public Mono<ApiResponse<UserDetails>> getUserDetailsById(@PathVariable String userId,
                                                              @RequestHeader(required = false) String traceId) {
        return userDetailsService.getUserDetailsById(userId)
                .map(userDetails -> ApiResponse.<UserDetails>builder()
                        .message("UserDetails retrieved successfully")
                        .traceId(traceId)
                        .result(userDetails)
                        .build());
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
                        .build());
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
                        .build()));
    }
}
