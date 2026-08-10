package com.dauducbach.clone.modules.media.configuration;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(MediaPolicyProperties.class)
public class MediaPolicyConfiguration {
}
