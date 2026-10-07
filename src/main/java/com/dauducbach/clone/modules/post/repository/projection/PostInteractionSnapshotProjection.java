package com.dauducbach.clone.modules.post.repository.projection;

public interface PostInteractionSnapshotProjection {
    String getPostId();
    Long getLikes();
    Long getComments();
    Long getReposts();
    Boolean getLikedByViewer();
    Boolean getRepostedByViewer();
}
