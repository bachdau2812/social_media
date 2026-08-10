package com.dauducbach.clone.utils;

import com.google.gson.JsonObject;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.experimental.NonFinal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Locale;

@Component
@RequiredArgsConstructor
@FieldDefaults(level = lombok.AccessLevel.PRIVATE, makeFinal = true)
public class MediaScanUtils {
    private static final Logger log = LoggerFactory.getLogger(MediaScanUtils.class);

    WebClient webClient;

    @NonFinal
    @Value("${post.media.scan.api-url:http://localhost:8000/api/v1/scan}")
    String scanApiUrl;

    @NonFinal
    @Value("${app.media.limits.image:100MB}")
    DataSize maxScanMemorySize;

    public Mono<ScanResult> scanMedia(String mediaUrl) {
        return scanMedia(mediaUrl, null);
    }

    public Mono<ScanResult> scanMedia(String mediaUrl, String publicId) {
        if (mediaUrl == null || mediaUrl.isBlank()) {
            log.warn("|MediaScanUtils|scanMedia|missing mediaUrl|publicId={}", publicId);
            return Mono.just(ScanResult.rejected());
        }

        return webClient.get()
                .uri(mediaUrl)
                .retrieve()
                .bodyToMono(byte[].class)
                .flatMap(bytes -> {
                    if (isOverMaxScanSize(bytes)) {
                        log.warn("|MediaScanUtils|scanMedia|rejected oversized media|publicId={}|byteSize={}|limit={}",
                                publicId, bytes.length, maxScanMemorySize.toBytes());
                        return Mono.just(ScanResult.rejected());
                    }
                    return callScanApi(bytes, publicId, mediaUrl);
                })
                .onErrorResume(error -> {
                    log.error("|MediaScanUtils|scanMedia|failed|publicId={}|mediaUrlLength={}|error={}",
                            publicId, mediaUrl.length(), error.getMessage());
                    return Mono.just(ScanResult.rejected());
                });
    }

    private boolean isOverMaxScanSize(byte[] bytes) {
        return maxScanMemorySize != null && bytes != null && bytes.length > maxScanMemorySize.toBytes();
    }

    private Mono<ScanResult> callScanApi(byte[] bytes, String publicId, String mediaUrl) {
        String filename = buildScanFilename(publicId, mediaUrl);
        MultiValueMap<String, Object> formData = new LinkedMultiValueMap<>();
        formData.add("file", new ByteArrayResource(bytes) {
            @Override
            public String getFilename() {
                return filename;
            }
        });

        return webClient.post()
                .uri(scanApiUrl)
                .contentType(MediaType.MULTIPART_FORM_DATA)
                .body(BodyInserters.fromMultipartData(formData))
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseScanResponse)
                .doOnSuccess(result -> log.info("|MediaScanUtils|callScanApi|completed|publicId={}|filename={}|nsfw={}",
                        publicId, filename, result.nsfw()))
                .onErrorResume(error -> {
                    log.error("|MediaScanUtils|callScanApi|failed|publicId={}|filename={}|error={}",
                            publicId, filename, error.getMessage());
                    return Mono.just(ScanResult.rejected());
                });
    }

    private ScanResult parseScanResponse(String rawResponse) {
        JsonObject json = GsonUtils.fromString(rawResponse);
        JsonObject data = json.getAsJsonObject("data");
        if (data == null) {
            return ScanResult.rejected();
        }

        boolean isNsfw = data.has("is_nsfw") && data.get("is_nsfw").getAsBoolean();
        return new ScanResult(isNsfw);
    }

    private String buildScanFilename(String publicId, String mediaUrl) {
        String filename = basename(publicId);
        if (filename.isBlank()) {
            filename = basename(mediaUrl);
        }
        if (filename.isBlank()) {
            filename = "media";
        }

        filename = filename.replaceAll("[^A-Za-z0-9._-]", "_");
        if (!hasExtension(filename)) {
            filename = filename + resolveExtension(mediaUrl);
        }
        return filename;
    }

    private String basename(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }

        String clean = value;
        int queryIndex = clean.indexOf('?');
        if (queryIndex >= 0) {
            clean = clean.substring(0, queryIndex);
        }

        int slashIndex = Math.max(clean.lastIndexOf('/'), clean.lastIndexOf('\\'));
        return slashIndex >= 0 ? clean.substring(slashIndex + 1) : clean;
    }

    private boolean hasExtension(String filename) {
        int dotIndex = filename.lastIndexOf('.');
        return dotIndex > 0 && dotIndex < filename.length() - 1;
    }

    private String resolveExtension(String mediaUrl) {
        String filenameFromUrl = basename(mediaUrl);
        int dotIndex = filenameFromUrl.lastIndexOf('.');
        if (dotIndex > 0 && dotIndex < filenameFromUrl.length() - 1) {
            String extension = filenameFromUrl.substring(dotIndex).toLowerCase(Locale.ROOT);
            if (extension.matches("\\.[a-z0-9]{1,8}")) {
                return extension;
            }
        }
        return ".jpg";
    }

    public record ScanResult(boolean nsfw) {
        public static ScanResult approved() {
            return new ScanResult(false);
        }

        public static ScanResult rejected() {
            return new ScanResult(true);
        }

        public boolean isNsfw() {
            return nsfw;
        }
    }
}
