package com.dauducbach.clone.modules.post.service.post;

import com.dauducbach.clone.infrastructure.vector.VectorMath;
import com.dauducbach.clone.modules.post.entity.*;
import com.dauducbach.clone.utils.GsonUtils;
import org.jsoup.Jsoup;
import org.springframework.stereotype.Component;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;

@Component
public class PostEmbeddingTextBuilder {
    public String build(PostDetails post, List<PostItem> items) {
        List<String> parts = new ArrayList<>();
        parts.add(plain(post.getContent()));
        Set<String> hashtags = new LinkedHashSet<>();
        List<String> sourceTags = post.getHashtagList();
        if (sourceTags != null) for (String tag : sourceTags) {
            String normalized = plain(tag).replaceFirst("^#+", "").toLowerCase(Locale.ROOT);
            if (!normalized.isBlank()) hashtags.add("#" + normalized);
        }
        parts.add(String.join(" ", hashtags));
        eligible(post, items).forEach(item -> parts.add(plain(item.getCaption())));
        return String.join("\n", parts.stream().filter(value -> !value.isBlank()).toList());
    }

    public String fingerprint(PostDetails post, List<PostItem> items) {
        return digest(VectorMath.MODEL + "\n" + VectorMath.DIMENSION + "\n" + build(post, items));
    }

    /** Includes source identities/timestamps so removed or replaced media fences an in-flight computation. */
    public String revision(PostDetails post, List<PostItem> items) {
        // Canonical primitive values avoid Gson reflecting into JDK java.time internals.
        List<Object> source = new ArrayList<>(Arrays.asList(post.getPostId(), post.getUserId(), post.getContent(),
                post.getHashtag(), stamp(post.getCreatedAt()), stamp(post.getUpdatedAt()), post.getValidateStatus(),
                post.getMusicId(), post.getMusicStart(), post.getMusicEnd(), post.getMediaRatio()));
        source.add(eligible(post, items).stream().map(item -> Arrays.asList(item.getId(), item.getPostId(),
                item.getOrderNumber(), item.getMediaId(), item.getCaption(), item.getMusicId(), item.getMusicStart(),
                item.getMusicEnd(), stamp(item.getCreatedAt()), stamp(item.getUpdatedAt()))).toList());
        return digest(GsonUtils.getGson().toJson(source));
    }
    private String stamp(java.time.Instant value) { return value == null ? null : value.toString(); }

    private List<PostItem> eligible(PostDetails post, List<PostItem> items) {
        // Moderation persists only approved items; deleted/rejected items are absent from this authoritative query.
        return items.stream().filter(Objects::nonNull)
                .filter(item -> Objects.equals(post.getPostId(), item.getPostId()))
                .filter(item -> item.getMediaId() != null && !item.getMediaId().isBlank())
                .sorted(Comparator.comparing(PostItem::getOrderNumber, Comparator.nullsLast(Integer::compareTo))
                        .thenComparing(PostItem::getId, Comparator.nullsLast(String::compareTo))).toList();
    }
    private String plain(String text) { return text == null ? "" : Jsoup.parse(text).text().replace('\u00a0', ' ').replaceAll("\\s+", " ").trim(); }
    private String digest(String text) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8))); }
        catch (java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
}
