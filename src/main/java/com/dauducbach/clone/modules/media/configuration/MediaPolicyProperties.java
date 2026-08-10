package com.dauducbach.clone.modules.media.configuration;

import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

@Getter
@Setter
@ConfigurationProperties(prefix = "app.media.limits")
public class MediaPolicyProperties {
    private DataSize image = DataSize.ofMegabytes(100);
    private DataSize video = DataSize.ofMegabytes(100);
    private DataSize audio = DataSize.ofMegabytes(50);

    @PostConstruct
    public void validate() {
        validatePositive("app.media.limits.image", image);
        validatePositive("app.media.limits.video", video);
        validatePositive("app.media.limits.audio", audio);
    }

    public long imageMaxBytes() {
        return image.toBytes();
    }

    public long videoMaxBytes() {
        return video.toBytes();
    }

    public long audioMaxBytes() {
        return audio.toBytes();
    }

    private void validatePositive(String property, DataSize value) {
        if (value == null || value.toBytes() <= 0) {
            throw new IllegalArgumentException(property + " must be greater than 0");
        }
    }
}
