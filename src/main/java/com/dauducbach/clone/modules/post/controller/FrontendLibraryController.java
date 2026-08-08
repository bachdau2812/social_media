package com.dauducbach.clone.modules.post.controller;

import com.dauducbach.clone.commons.response.ApiResponse;
import com.dauducbach.clone.commons.security.ActorIdentity;
import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.modules.post.dto.request.ArchiveContentRequest;
import com.dauducbach.clone.modules.post.dto.request.SavePostRequest;
import com.dauducbach.clone.modules.post.dto.request.UpsertDraftRequest;
import com.dauducbach.clone.modules.post.entity.SavedItem;
import com.dauducbach.clone.modules.post.entity.UserArchiveItem;
import com.dauducbach.clone.modules.post.entity.UserDraft;
import com.dauducbach.clone.modules.post.service.post.LibraryService;
import jakarta.validation.Valid;
import org.springframework.security.core.Authentication;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequiredArgsConstructor
@RequestMapping("/me/{userId}")
public class FrontendLibraryController {
    private final LibraryService service;

    @GetMapping("/saved")
    public Mono<ApiResponse<PageResponse<SavedItem>>> getSaved(@PathVariable String userId,
            Authentication authentication, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        return service.getSaved(requireUser(authentication, userId), page, size).map(result -> ApiResponse.<PageResponse<SavedItem>>builder().message("Saved posts fetched").result(result).build());
    }

    @PostMapping("/saved/items")
    public Mono<ApiResponse<SavedItem>> savePost(@PathVariable String userId,
            Authentication authentication, @Valid @RequestBody SavePostRequest request) {
        return service.savePost(requireUser(authentication, userId), request).map(result -> ApiResponse.<SavedItem>builder().message("Post saved").result(result).build());
    }

    @DeleteMapping("/saved/items/{postId}")
    public Mono<ApiResponse<String>> removeSavedPost(@PathVariable String userId,
            Authentication authentication, @PathVariable String postId) {
        return service.removeSavedPost(requireUser(authentication, userId), postId).map(result -> ApiResponse.<String>builder().message("Saved post removed").result(result).build());
    }

    @GetMapping("/drafts")
    public Mono<ApiResponse<List<UserDraft>>> getDrafts(@PathVariable String userId,
            Authentication authentication) {
        return service.getDrafts(requireUser(authentication, userId)).map(result -> ApiResponse.<List<UserDraft>>builder().message("Drafts fetched").result(result).build());
    }

    @PostMapping("/drafts")
    public Mono<ApiResponse<UserDraft>> upsertDraft(@PathVariable String userId,
            Authentication authentication, @RequestBody UpsertDraftRequest request) {
        return service.upsertDraft(requireUser(authentication, userId), request).map(result -> ApiResponse.<UserDraft>builder().message("Draft saved").result(result).build());
    }

    @DeleteMapping("/drafts/{draftId}")
    public Mono<ApiResponse<String>> deleteDraft(@PathVariable String userId,
            Authentication authentication, @PathVariable String draftId) {
        return service.deleteDraft(requireUser(authentication, userId), draftId).map(result -> ApiResponse.<String>builder().message("Draft deleted").result(result).build());
    }

    @GetMapping("/archive")
    public Mono<ApiResponse<List<UserArchiveItem>>> getArchive(@PathVariable String userId,
            Authentication authentication, @RequestParam(required = false) String type) {
        return service.getArchive(requireUser(authentication, userId), type).map(result -> ApiResponse.<List<UserArchiveItem>>builder().message("Archive fetched").result(result).build());
    }

    @PostMapping("/archive")
    public Mono<ApiResponse<UserArchiveItem>> archiveContent(@PathVariable String userId,
            Authentication authentication, @Valid @RequestBody ArchiveContentRequest request) {
        return service.archiveContent(requireUser(authentication, userId), request).map(result -> ApiResponse.<UserArchiveItem>builder().message("Content archived").result(result).build());
    }

    @PostMapping("/archive/{contentId}/restore")
    public Mono<ApiResponse<String>> restoreArchiveItem(@PathVariable String userId,
            Authentication authentication, @PathVariable String contentId) {
        return service.restoreArchiveItem(requireUser(authentication, userId), contentId).map(result -> ApiResponse.<String>builder().message("Archive item restored").result(result).build());
    }

    @DeleteMapping("/archive/items/{archiveItemId}")
    public Mono<ApiResponse<String>> deleteArchiveItem(@PathVariable String userId,
            Authentication authentication, @PathVariable String archiveItemId) {
        return service.deleteArchiveItem(requireUser(authentication, userId), archiveItemId).map(result -> ApiResponse.<String>builder().message("Archive item deleted").result(result).build());
    }
    private String requireUser(Authentication authentication, String userId) {
        return ActorIdentity.require(authentication.getName(), userId);
    }
}
