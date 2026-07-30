package com.dauducbach.clone.modules.media.service;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public final class CloudinaryUtils {
    private static final String CLOUDINARY_HOST = "res.cloudinary.com/";
    private static final String VERSION_PATTERN = "v\\d+";

    private CloudinaryUtils() {
    }

    public static String withTransformations(String mediaUrl, String... transformations) {
        List<String> normalizedTransformations = Arrays.stream(transformations == null ? new String[0] : transformations)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .toList();
        return withTransformations(mediaUrl, normalizedTransformations);
    }

    public static String withTransformations(String mediaUrl, List<String> transformations) {
        if (mediaUrl == null || mediaUrl.isBlank()) {
            throw new IllegalArgumentException("mediaUrl is required");
        }
        List<String> normalizedTransformations = transformations == null
                ? List.of()
                : transformations.stream()
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(value -> !value.isBlank())
                .toList();
        if (normalizedTransformations.isEmpty()) {
            return mediaUrl.trim();
        }

        UrlParts urlParts = splitSuffix(mediaUrl.trim());
        String urlWithoutSuffix = urlParts.urlWithoutSuffix();
        int cloudinaryIndex = urlWithoutSuffix.indexOf(CLOUDINARY_HOST);
        if (cloudinaryIndex < 0) {
            throw new IllegalArgumentException("mediaUrl is not a Cloudinary delivery url");
        }

        String prefix = urlWithoutSuffix.substring(0, cloudinaryIndex + CLOUDINARY_HOST.length());
        String cloudinaryPath = urlWithoutSuffix.substring(cloudinaryIndex + CLOUDINARY_HOST.length());
        String[] segments = cloudinaryPath.split("/");
        if (segments.length < 4) {
            throw new IllegalArgumentException("Cloudinary delivery url is invalid");
        }

        int versionIndex = findVersionIndex(segments);
        if (versionIndex < 3) {
            throw new IllegalArgumentException("Cloudinary delivery url must contain a version segment");
        }

        String transformationText = String.join(",", normalizedTransformations);
        StringBuilder result = new StringBuilder(prefix)
                .append(segments[0])
                .append('/')
                .append(segments[1])
                .append('/')
                .append(segments[2])
                .append('/');

        if (versionIndex == 3) {
            result.append(transformationText).append('/');
        } else {
            result.append(joinSegments(segments, 3, versionIndex))
                    .append(',')
                    .append(transformationText)
                    .append('/');
        }

        result.append(joinSegments(segments, versionIndex, segments.length));
        return result.append(urlParts.suffix()).toString();
    }

    public static String withAudioSegment(String musicUrl, Long musicStart, Long musicEnd) {
        if (musicStart == null && musicEnd == null) {
            return musicUrl;
        }
        validateAudioSegment(musicStart, musicEnd);
        long duration = musicEnd - musicStart;
        return withTransformations(musicUrl, "so_" + musicStart, "du_" + duration);
    }

    public static boolean isCloudinaryDeliveryUrl(String mediaUrl) {
        return mediaUrl != null && mediaUrl.toLowerCase(Locale.ROOT).contains(CLOUDINARY_HOST);
    }

    public static void validateAudioSegment(Long musicStart, Long musicEnd) {
        if (musicStart == null || musicEnd == null) {
            throw new IllegalArgumentException("musicStart and musicEnd must be provided together");
        }
        if (musicStart < 0) {
            throw new IllegalArgumentException("musicStart must be greater than or equal to 0");
        }
        if (musicEnd <= musicStart) {
            throw new IllegalArgumentException("musicEnd must be greater than musicStart");
        }
    }

    private static int findVersionIndex(String[] segments) {
        for (int i = 3; i < segments.length; i++) {
            if (segments[i].matches(VERSION_PATTERN)) {
                return i;
            }
        }
        return -1;
    }

    private static String joinSegments(String[] segments, int startInclusive, int endExclusive) {
        return String.join("/", Arrays.copyOfRange(segments, startInclusive, endExclusive));
    }

    private static UrlParts splitSuffix(String mediaUrl) {
        int queryIndex = mediaUrl.indexOf('?');
        int fragmentIndex = mediaUrl.indexOf('#');
        int suffixIndex;
        if (queryIndex < 0) {
            suffixIndex = fragmentIndex;
        } else if (fragmentIndex < 0) {
            suffixIndex = queryIndex;
        } else {
            suffixIndex = Math.min(queryIndex, fragmentIndex);
        }

        if (suffixIndex < 0) {
            return new UrlParts(mediaUrl, "");
        }
        return new UrlParts(mediaUrl.substring(0, suffixIndex), mediaUrl.substring(suffixIndex));
    }

    private record UrlParts(String urlWithoutSuffix, String suffix) {
    }
}
