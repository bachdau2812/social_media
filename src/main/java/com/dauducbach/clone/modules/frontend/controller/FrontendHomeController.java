package com.dauducbach.clone.modules.frontend.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.frontend.dto.HomeScreenResponse;
import com.dauducbach.clone.modules.frontend.service.HomeScreenService;
import com.dauducbach.clone.modules.media.constant.MediaDisplayType;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/home")
public class FrontendHomeController {
    private final HomeScreenService service;

    @GetMapping
    public Mono<ApiResponse<HomeScreenResponse>> getHome(
            @RequestParam String userId,
            Authentication authentication,
            @RequestParam(defaultValue = "DISCOVER") String tab,
            @RequestParam(defaultValue = "20") int limit,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "FEED") MediaDisplayType mediaType
    ) {
        return service.getHome(requireUser(authentication, userId), tab, limit, page, mediaType)
                .map(result -> ApiResponse.<HomeScreenResponse>builder()
                        .message("Home screen fetched")
                        .result(result)
                        .build());
    }
    private String requireUser(Authentication authentication, String userId) {
        return ActorIdentity.require(authentication.getName(), userId);
    }
}
