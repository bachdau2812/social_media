package com.dauducbach.clone.modules.post.stories.publishing;

import java.util.List;
import java.util.Locale;

final class StoryMediaMetadata {
    private StoryMediaMetadata() { }

    static String resolvePublicId(String mediaUrl) {
        String normalized = mediaUrl == null ? "" : mediaUrl.trim();
        int uploadIndex = normalized.indexOf("/upload/");
        if (uploadIndex < 0) return stripExtension(basename(normalized));

        String[] parts = normalized.substring(uploadIndex + "/upload/".length()).split("/");
        int versionIndex = -1;
        for (int i = 0; i < parts.length; i++) {
            if (parts[i].matches("v\\d+")) {
                versionIndex = i;
                break;
            }
        }

        String publicPath = versionIndex >= 0 && versionIndex < parts.length - 1
                ? String.join("/", List.of(parts).subList(versionIndex + 1, parts.length))
                : parts[parts.length - 1];
        int queryIndex = publicPath.indexOf('?');
        if (queryIndex >= 0) publicPath = publicPath.substring(0, queryIndex);
        return stripExtension(publicPath);
    }

    static String resolveMediaType(String mediaUrl) {
        String filename = basename(mediaUrl);
        int dotIndex = filename.lastIndexOf('.');
        String extension = dotIndex > 0 && dotIndex < filename.length() - 1
                ? filename.substring(dotIndex).toLowerCase(Locale.ROOT)
                : ".jpg";
        if (!extension.matches("\\.[a-z0-9]{1,8}")) extension = ".jpg";
        return switch (extension) {
            case ".mp4", ".mov", ".webm", ".m4v" -> "VIDEO";
            default -> "IMAGE";
        };
    }

    private static String basename(String value) {
        if (value == null || value.isBlank()) return "";
        String clean = value;
        int queryIndex = clean.indexOf('?');
        if (queryIndex >= 0) clean = clean.substring(0, queryIndex);
        int slashIndex = Math.max(clean.lastIndexOf('/'), clean.lastIndexOf('\\'));
        return slashIndex >= 0 ? clean.substring(slashIndex + 1) : clean;
    }

    private static String stripExtension(String value) {
        int dotIndex = value.lastIndexOf('.');
        return dotIndex > 0 ? value.substring(0, dotIndex) : value;
    }
}
