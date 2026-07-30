package com.dauducbach.clone.modules.notification.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.modules.notification.dto.response.NotificationItemResponse;
import com.dauducbach.clone.modules.notification.service.NotificationQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/notifications")
public class NotificationQueryController {
    private final NotificationQueryService service;

    @GetMapping
    public Mono<ApiResponse<PageResponse<NotificationItemResponse>>> getNotifications(
            @RequestParam String userId,
            Authentication authentication,
            @RequestParam(defaultValue = "ALL") String filter,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size
    ) {
        return service.getNotifications(requireUser(authentication, userId), filter, page, size)
                .map(result -> ApiResponse.<PageResponse<NotificationItemResponse>>builder().message("Notifications fetched").result(result).build());
    }

    @GetMapping("/unread-count")
    public Mono<ApiResponse<Long>> unreadCount(@RequestParam String userId, Authentication authentication) {
        return service.unreadCount(requireUser(authentication, userId))
                .map(result -> ApiResponse.<Long>builder().message("Unread notifications counted").result(result).build());
    }

    @PostMapping("/{notificationId}/read")
    public Mono<ApiResponse<String>> markRead(@PathVariable String notificationId, Authentication authentication) {
        return service.markRead(notificationId, authentication.getName())
                .map(result -> ApiResponse.<String>builder().message("Notification marked read").result(result).build());
    }

    @PostMapping("/read-all")
    public Mono<ApiResponse<String>> markAllRead(@RequestParam String userId, Authentication authentication) {
        return service.markAllRead(requireUser(authentication, userId))
                .map(result -> ApiResponse.<String>builder().message("Notifications marked read").result(result).build());
    }
    private String requireUser(Authentication authentication, String userId) {
        return ActorIdentity.require(authentication.getName(), userId);
    }
}
