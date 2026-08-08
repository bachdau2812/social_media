package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.commons.response.PageResponse;
import com.dauducbach.clone.commons.exception.AppException;
import com.dauducbach.clone.commons.exception.ErrorCode;
import com.dauducbach.clone.modules.post.dto.request.ArchiveContentRequest;
import com.dauducbach.clone.modules.post.dto.request.SavePostRequest;
import com.dauducbach.clone.modules.post.dto.request.UpsertDraftRequest;
import com.dauducbach.clone.modules.post.entity.SavedItem;
import com.dauducbach.clone.modules.post.entity.PostDetails;
import com.dauducbach.clone.modules.post.entity.UserArchiveItem;
import com.dauducbach.clone.modules.post.entity.UserDraft;
import com.dauducbach.clone.modules.post.repositoty.SavedItemRepository;
import com.dauducbach.clone.modules.post.repositoty.PostDetailsRepository;
import com.dauducbach.clone.modules.post.repositoty.UserArchiveItemRepository;
import com.dauducbach.clone.modules.post.repositoty.UserDraftRepository;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class LibraryService {
    SavedItemRepository savedItemRepository;
    UserDraftRepository draftRepository;
    UserArchiveItemRepository archiveRepository;
    PostDetailsRepository postDetailsRepository;
    R2dbcEntityTemplate r2dbcEntityTemplate;

    public Mono<PageResponse<SavedItem>> getSaved(String userId, int page, int size) {
        int safePage = Math.max(page, 0);
        int safeSize = size <= 0 ? 20 : Math.min(size, 100);
        int offset = safePage * safeSize;
        return savedItemRepository.countByUserId(userId)
                .flatMap(total -> savedItemRepository.findByUserId(userId, safeSize, offset)
                        .collectList()
                        .map(items -> PageResponse.of(items, safePage, total, safeSize)));
    }

    public Mono<SavedItem> savePost(String userId, SavePostRequest request) {
        SavedItem item = SavedItem.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .postId(request.postId())
                .createdAt(Instant.now())
                .build();
        return r2dbcEntityTemplate.insert(SavedItem.class).using(item);
    }

    public Mono<String> removeSavedPost(String userId, String postId) {
        return savedItemRepository.deleteByUserIdAndPostId(userId, postId).thenReturn("OK");
    }

    public Mono<java.util.List<UserDraft>> getDrafts(String userId) {
        return draftRepository.findByUserId(userId).collectList();
    }

    public Mono<UserDraft> upsertDraft(String userId, UpsertDraftRequest request) {
        Instant now = Instant.now();
        boolean creating = request.id() == null || request.id().isBlank();
        String id = creating ? UUID.randomUUID().toString() : request.id();
        UserDraft draft = buildDraft(userId, request, id, now, now);

        if (creating) {
            return r2dbcEntityTemplate.insert(UserDraft.class).using(draft);
        }

        return draftRepository.findById(id)
                .filter(existing -> userId.equals(existing.getUserId()))
                .flatMap(existing -> {
                    existing.setDraftType(draft.getDraftType());
                    existing.setThumbnailUrl(draft.getThumbnailUrl());
                    existing.setMediaCount(draft.getMediaCount());
                    existing.setCaptionPreview(draft.getCaptionPreview());
                    existing.setPayload(draft.getPayload());
                    existing.setUpdatedAt(now);
                    return draftRepository.save(existing);
                })
                .switchIfEmpty(r2dbcEntityTemplate.insert(UserDraft.class).using(draft));
    }

    private UserDraft buildDraft(String userId, UpsertDraftRequest request, String id, Instant createdAt, Instant updatedAt) {
        return UserDraft.builder()
                .id(id)
                .userId(userId)
                .draftType(request.draftType() == null || request.draftType().isBlank() ? "POST" : request.draftType())
                .thumbnailUrl(request.thumbnailUrl())
                .mediaCount(Math.max(request.mediaCount(), 0))
                .captionPreview(request.captionPreview())
                .payload(request.payload())
                .createdAt(createdAt)
                .updatedAt(updatedAt)
                .build();
    }

    public Mono<String> deleteDraft(String userId, String draftId) {
        return draftRepository.deleteById(draftId).thenReturn("OK");
    }

    public Mono<java.util.List<UserArchiveItem>> getArchive(String userId, String type) {
        return archiveRepository.findByUserIdAndType(userId, type).collectList();
    }

    public Mono<UserArchiveItem> archiveContent(String userId, ArchiveContentRequest request) {
        UserArchiveItem item = UserArchiveItem.builder()
                .id(UUID.randomUUID().toString())
                .userId(userId)
                .contentId(request.contentId())
                .contentType(request.contentType())
                .thumbnailUrl(request.thumbnailUrl())
                .captionPreview(request.captionPreview())
                .archivedAt(Instant.now())
                .build();
        if (!"POST".equalsIgnoreCase(request.contentType())) {
            return r2dbcEntityTemplate.insert(UserArchiveItem.class).using(item);
        }
        return postDetailsRepository.findById(request.contentId())
                .filter(post -> userId.equals(post.getUserId()))
                .switchIfEmpty(Mono.error(new AppException(ErrorCode.POST_UPDATE_FAILED, "Only the post owner can archive this post")))
                .flatMap(post -> {
                    post.setValidateStatus("ARCHIVED");
                    post.setUpdatedAt(Instant.now());
                    return postDetailsRepository.save(post);
                })
                .then(r2dbcEntityTemplate.insert(UserArchiveItem.class).using(item));
    }

    public Mono<String> restoreArchiveItem(String userId, String contentId) {
        return archiveRepository.findByUserIdAndContentId(userId, contentId)
                .flatMap(item -> {
                    Mono<PostDetails> restorePost = "POST".equalsIgnoreCase(item.getContentType())
                            ? postDetailsRepository.findById(contentId)
                                .filter(post -> userId.equals(post.getUserId()))
                                .flatMap(post -> {
                                    post.setValidateStatus("APPROVED");
                                    post.setUpdatedAt(Instant.now());
                                    return postDetailsRepository.save(post);
                                })
                            : Mono.empty();
                    return restorePost.then(archiveRepository.restore(userId, contentId)).thenReturn("OK");
                })
                .defaultIfEmpty("OK");
    }

    public Mono<String> deleteArchiveItem(String userId, String archiveItemId) {
        return archiveRepository.deleteByUserIdAndId(userId, archiveItemId).thenReturn("OK");
    }
}
