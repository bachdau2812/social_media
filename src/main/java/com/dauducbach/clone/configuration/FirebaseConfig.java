package com.dauducbach.clone.configuration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class FirebaseConfig {
    private static final Logger log = LoggerFactory.getLogger(FirebaseConfig.class);

    private final boolean enabled;
    private final String credentialsPath;

    public FirebaseConfig(
            @Value("${firebase.enabled:false}") boolean enabled,
            @Value("${firebase.credentials-path:}") String credentialsPath
    ) {
        this.enabled = enabled;
        this.credentialsPath = credentialsPath;
    }

    @PostConstruct
    public void init() throws Exception {
        if (!enabled) {
            log.warn("|FirebaseConfig|init|disabled; device push is unavailable; set FIREBASE_ENABLED=true "
                    + "and configure FIREBASE_CREDENTIALS_PATH or GOOGLE_APPLICATION_CREDENTIALS");
            return;
        }

        if (FirebaseApp.getApps().isEmpty()) {
            FirebaseApp.initializeApp(FirebaseOptions.builder()
                    .setCredentials(loadCredentials())
                    .build());
            log.info("|FirebaseConfig|init|initialized");
        }
    }

    private GoogleCredentials loadCredentials() throws IOException {
        if (!StringUtils.hasText(credentialsPath)) {
            return GoogleCredentials.getApplicationDefault();
        }
        try (InputStream serviceAccount = Files.newInputStream(Path.of(credentialsPath))) {
            return GoogleCredentials.fromStream(serviceAccount);
        }
    }
}
