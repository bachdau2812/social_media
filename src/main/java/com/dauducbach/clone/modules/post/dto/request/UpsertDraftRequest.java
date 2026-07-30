package com.dauducbach.clone.modules.post.dto.request;

public record UpsertDraftRequest(String id, String draftType, String thumbnailUrl, int mediaCount, String captionPreview, String payload) {
}