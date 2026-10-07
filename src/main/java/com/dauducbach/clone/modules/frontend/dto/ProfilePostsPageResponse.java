package com.dauducbach.clone.modules.frontend.dto;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import java.util.List;
public record ProfilePostsPageResponse(String userId, List<TimelinePostResponse> posts, int pageNumber,
                                       int pageSize, boolean hasMore, boolean hasPrevious, boolean selectedPostFound) {
    public record TimelinePostResponse(@JsonUnwrapped ProfilePostResponse post, boolean savedByCurrentUser) {}
}
