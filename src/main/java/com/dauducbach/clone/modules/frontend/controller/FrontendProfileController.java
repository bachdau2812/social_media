package com.dauducbach.clone.modules.frontend.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.frontend.dto.ConnectionsResponse;
import com.dauducbach.clone.modules.frontend.dto.ProfileSummaryResponse;
import com.dauducbach.clone.modules.frontend.service.ProfileScreenService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/profiles")
public class FrontendProfileController {
    private final ProfileScreenService service;

    @GetMapping("/{userId}/summary")
    public Mono<ApiResponse<ProfileSummaryResponse>> getProfile(
            @PathVariable String userId,
            @RequestParam(required = false) String viewerId,
            Authentication authentication,
            @RequestParam(defaultValue = "12") int postLimit
    ) {
        String resolvedViewerId = viewerId == null || viewerId.isBlank()
                ? authentication.getName()
                : ActorIdentity.require(authentication.getName(), viewerId);
        return service.getProfile(resolvedViewerId, userId, postLimit)
                .map(result -> ApiResponse.<ProfileSummaryResponse>builder()
                        .message("Profile summary fetched")
                        .result(result)
                        .build());
    }

    @GetMapping("/{userId}/connections")
    public Mono<ApiResponse<ConnectionsResponse>> getConnections(
            @PathVariable String userId,
            @RequestParam(required = false) String viewerId,
            Authentication authentication,
            @RequestParam(defaultValue = "FOLLOWERS") String tab,
            @RequestParam(defaultValue = "") String query,
            @RequestParam(defaultValue = "RECENT") String sort,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        String resolvedViewerId = viewerId == null || viewerId.isBlank()
                ? authentication.getName()
                : ActorIdentity.require(authentication.getName(), viewerId);
        return service.getConnections(resolvedViewerId, userId, tab, query, sort, page, size)
                .map(result -> ApiResponse.<ConnectionsResponse>builder()
                        .message("Connections fetched")
                        .result(result)
                        .build());
    }
}
