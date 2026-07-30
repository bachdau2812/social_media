package com.dauducbach.clone.modules.user.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.user.entity.UserSettings;
import com.dauducbach.clone.modules.user.service.UserSettingsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/me/{userId}/settings")
public class FrontendSettingsController {
    private final UserSettingsService service;

    @GetMapping
    public Mono<ApiResponse<UserSettings>> getSettings(@PathVariable String userId,
            Authentication authentication) {
        return service.getSettings(requireUser(authentication, userId))
                .map(result -> ApiResponse.<UserSettings>builder().message("Settings fetched").result(result).build());
    }

    @PatchMapping
    public Mono<ApiResponse<UserSettings>> patchSettings(@PathVariable String userId,
            Authentication authentication, @RequestBody UserSettings request) {
        return service.updateSettings(requireUser(authentication, userId), request)
                .map(result -> ApiResponse.<UserSettings>builder().message("Settings updated").result(result).build());
    }
    private String requireUser(Authentication authentication, String userId) {
        return ActorIdentity.require(authentication.getName(), userId);
    }
}
