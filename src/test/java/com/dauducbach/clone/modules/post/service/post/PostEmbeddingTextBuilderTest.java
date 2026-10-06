package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.modules.post.entity.*;
import org.junit.jupiter.api.Test;
import java.util.List;
import static org.assertj.core.api.Assertions.*;

class PostEmbeddingTextBuilderTest {
    // Reflection allows a behavioral contract failure before the new implementation exists.
    String build(PostDetails post, List<PostItem> items) throws Exception {
        Class<?> type;
        try { type = Class.forName("com.dauducbach.clone.modules.post.service.post.PostEmbeddingTextBuilder"); }
        catch (ClassNotFoundException e) { fail("Whole-post text builder is not implemented"); return null; }
        return (String) type.getMethod("build", PostDetails.class, List.class).invoke(type.getConstructor().newInstance(), post, items);
    }
    @Test void includesPlainContentDeduplicatedHashtagsAndAllCurrentCaptionsInOrder() throws Exception {
        PostDetails post = PostDetails.builder().postId("p").content("<p>Hello &amp; world</p>").hashtag("[\"Java\",\"#java\",\"cats\"]").build();
        assertThat(build(post, List.of(
                item("b", "p", 2, "last <b>caption</b>"), item("a", "p", 1, "first"), item("x", "other", 0, "removed"))))
                .isEqualTo("Hello & world\n#java #cats\nfirst\nlast caption");
    }
    @Test void emptyPostHasNoInventedInput() throws Exception {
        assertThat(build(PostDetails.builder().postId("p").content("<p> </p>").build(), List.of())).isEmpty();
    }
    @Test void fingerprintsChangeForHashtagsCaptionsAndRemovedItemsAndRevisionIncludesTimestamps() {
        PostEmbeddingTextBuilder builder = new PostEmbeddingTextBuilder();
        PostDetails post = PostDetails.builder().postId("p").content("hello").updatedAt(java.time.Instant.EPOCH).build();
        PostItem item = item("a", "p", 1, "caption");
        String before = builder.fingerprint(post, List.of(item));
        post.setHashtag("[\"tag\"]"); assertThat(builder.fingerprint(post, List.of(item))).isNotEqualTo(before);
        before = builder.fingerprint(post, List.of(item)); item.setCaption("new caption");
        assertThat(builder.fingerprint(post, List.of(item))).isNotEqualTo(before);
        assertThat(builder.fingerprint(post, List.of())).isNotEqualTo(builder.fingerprint(post, List.of(item)));
        String revision = builder.revision(post, List.of(item)); post.setUpdatedAt(java.time.Instant.EPOCH.plusSeconds(1));
        assertThat(builder.revision(post, List.of(item))).isNotEqualTo(revision);
    }
    static PostItem item(String id, String postId, int order, String caption) {
        return PostItem.builder().id(id).postId(postId).orderNumber(order).mediaId("media-"+id).caption(caption).build();
    }
}
